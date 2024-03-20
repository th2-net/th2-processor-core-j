/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.processor.strategy

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.processor.Application
import com.exactpro.th2.processor.Application.Companion.CONFIGURATION_ERROR_PREFIX
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.Crawler
import com.exactpro.th2.processor.core.configuration.CrawlerConfiguration
import com.exactpro.th2.processor.core.event.EventCrawler
import com.exactpro.th2.processor.core.state.CrawlerState
import com.exactpro.th2.processor.utility.log
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant

abstract class AbstractCrawlerStrategy(context: Context): AbstractStrategy(context) {

    private val crawlerConfig: CrawlerConfiguration = requireNotNull(context.configuration.crawler) {
        CONFIGURATION_ERROR_PREFIX + "the `crawler` setting can be null"
    }
    private val eventRouter = context.commonFactory.eventBatchRouter
    private val processor: IProcessor
    private val crawlers: Set<Crawler<*>>

    private val from: Instant = crawlerConfig.from
    private val to: Instant? = crawlerConfig.to
    private val intervalLength: Duration = crawlerConfig.intervalLength
    private val intervalProcessingDelay: Duration = crawlerConfig.intervalPrecessingDelay

    private var currentFrom: Instant
    private var currentTo: Instant

    init {
        currentFrom = from
        currentTo = from.doStep()

        processor = recoverState(CrawlerState::class.java)
            ?.let { state ->
                if (!doStepAndCheck(context.processorEventId, state.timestamp)) {
                    UNSET_PROCESSOR
                } else {
                    createProcessor(state.processorState)
                }
            } ?: createProcessor(null)

        if (processor === UNSET_PROCESSOR) {
            crawlers = emptySet()
        } else {
            crawlers = mutableSetOf<Crawler<*>>().apply {
                crawlerConfig.messages?.let { add(createCradleMessageGroupCrawler(context, processor)) }
                crawlerConfig.events?.let { add(EventCrawler(context, processor)) }
            }

            check(crawlers.isNotEmpty()) {
                "Neither of `messages`, `events` options are filled. Processor must work in any mode"
            }
            K_LOGGER.info { "Processing started" }
            readiness.enable()
        }
    }

    override fun run() {
        if (processor === UNSET_PROCESSOR) {
            return
        }

        do {
            checkProcessingDelayAndWait()

            val intervalEventId: EventID = reportStartProcessing(context.processorEventId)
            crawlers.parallelStream().forEach { crawler ->
                crawler.processInterval(currentFrom, currentTo, intervalEventId)
            }
            storeState(intervalEventId, CrawlerState(currentTo, processor.serializeState()))
            reportEndProcessing(intervalEventId)

            if (!doStepAndCheck(context.processorEventId, currentTo)) {
                break
            }
        } while (isActive)
    }

    override fun close() {
        super.close()
        crawlers.forEach { crawler ->
            runCatching(crawler::close).onFailure { e ->
                K_LOGGER.error(e) { "Closing ${crawler::class.java.simpleName} failure" }
            }
        }
        runCatching(processor::close).onFailure { e ->
            K_LOGGER.error(e) { "Closing ${processor::class.java.simpleName} failure" }
        }
    }

    internal abstract fun createCradleMessageGroupCrawler(context: Context, processor: IProcessor): Crawler<*>

    private fun checkProcessingDelayAndWait() {
        val currentTime = context.timeSource.instant()
        val untilIntervalEnd = Duration.between(currentTo.plus(intervalProcessingDelay), currentTime)
        if (untilIntervalEnd < Duration.ZERO) {
            val absTimeToWait = untilIntervalEnd.abs()
            K_LOGGER.info {
                "Start processing interval from $currentFrom to $currentTo after $absTimeToWait. " +
                        "Current time: $currentTime; processing delay: $intervalProcessingDelay"
            }
            reportWaitBeforeProcessing(absTimeToWait, context.processorEventId)
            val waitingTimeMillis = absTimeToWait.toMillis()
            Thread.sleep(waitingTimeMillis)
        }
    }

    private fun doStepAndCheck(processorEventId: EventID, from: Instant): Boolean {
        currentFrom = from
        currentTo = currentFrom.doStep()
        if (currentFrom == currentTo) {
            reportProcessingComplete(crawlerConfig, processorEventId)
            return false
        }
        return true
    }

    private fun reportWaitBeforeProcessing(duration: Duration, processorEventId: EventID) {
        Event.start()
            .name("Waiting for '$duration' before processing interval [$currentFrom - $currentTo)")
            .type(Application.EVENT_TYPE_PROCESS_INTERVAL)
            .endTimestamp()
            .toProto(processorEventId)
            .also(context.eventBatcher::onEvent)
    }

    private fun reportProcessingComplete(crawlerConfig: CrawlerConfiguration, processorEventId: EventID) = Event.start()
        .name("Whole time range is processed [${crawlerConfig.from} - ${crawlerConfig.to})")
        .type(Application.EVENT_TYPE_PROCESS_INTERVAL)
        .toBatchProto(processorEventId)
        .log(K_LOGGER)
        .also(eventRouter::sendAll)

    private fun reportStartProcessing(processorEventId: EventID) = Event.start()
        .name("Process interval [$currentFrom - $currentTo)")
        .type(Application.EVENT_TYPE_PROCESS_INTERVAL)
        .toBatchProto(processorEventId)
        .log(K_LOGGER)
        .also(eventRouter::sendAll)
        .run { getEvents(0).id }

    private fun reportEndProcessing(intervalEventId: EventID) = Event.start()
        .name("Complete processing")
        .type(Application.EVENT_TYPE_PROCESS_INTERVAL)
        .toBatchProto(intervalEventId)
        .log(K_LOGGER)
        .also(eventRouter::sendAll)
        .run { getEvents(0).id }

    private fun Instant.doStep(): Instant {
        if (to == this) {
            return this
        }

        val next = this.plus(intervalLength)
        return when {
            to != null && to < next -> to
            else -> next
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private val UNSET_PROCESSOR = object : IProcessor {}
    }
}