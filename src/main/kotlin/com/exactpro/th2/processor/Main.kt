/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.processor

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.metrics.registerLiveness
import com.exactpro.th2.common.metrics.registerReadiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.event.EventCrawler
import com.exactpro.th2.processor.core.message.GroupMessageCrawler
import com.exactpro.th2.processor.core.state.CrawlerState
import com.exactpro.th2.processor.core.state.DataProviderStateStorage
import com.exactpro.th2.processor.utility.load
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.system.exitProcess

private val K_LOGGER = KotlinLogging.logger {}

fun main(args: Array<String>) {
    try {
        val resources: Deque<() -> Unit> = ConcurrentLinkedDeque()
        Runtime.getRuntime().addShutdownHook(thread(start = false, name = "shutdown") {
            try {
                K_LOGGER.info { "Shutdown start" }
                resources.descendingIterator().forEach { action ->
                    runCatching(action).onFailure { K_LOGGER.error(it.message, it) }
                }
            } finally {
                K_LOGGER.info { "Shutdown end" }
            }
        })

        Box(resources, args).run()
    } catch (e: InterruptedException) {
        K_LOGGER.error(e) { "Message handling interrupted" }
    } catch (e: Throwable) {
        K_LOGGER.error(e) { "fatal error. Exit the program" }
        e.printStackTrace()
        exitProcess(1)
    }
}

class Box(
    resources: Deque<() -> Unit>,
    args: Array<String>
) {
    private val commonFactory = CommonFactory.createFromArguments(*args).apply {
        resources.add {
            K_LOGGER.info { "Closing common factory" }
            close()
            liveness.disable()
        }
    }
    private val scheduler = Executors.newScheduledThreadPool(1).apply {
        resources.add {
            K_LOGGER.info { "Shutdown scheduler" }
            shutdown()
            if (!awaitTermination(1, TimeUnit.SECONDS)) {
                K_LOGGER.warn { "Shutdown scheduler failure after 1 second" }
                shutdownNow()
            }
        }
    }

    @Suppress("SpellCheckingInspection")
    private val liveness = registerLiveness("main")
    private val readiness = registerReadiness("main")
    private val configuration = Configuration.create(commonFactory)
    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter.apply {
        resources.add {
            K_LOGGER.info { "Close event router" }
            close()
        }
    }
    private val eventBatcher = EventBatcher(executor = scheduler, onBatch = eventRouter::sendAll).apply {
        resources.add {
            K_LOGGER.info { "Close event batcher" }
            close()
        }
    }
    private val messageRouter = commonFactory.messageRouterMessageGroupBatch.apply {
        resources.add {
            K_LOGGER.info { "Close message router" }
            close()
        }
    }
    private val stateStorage = DataProviderStateStorage(
        messageRouter,
        commonFactory.grpcRouter.getService(DataProviderService::class.java),
        configuration.stateSessionAlias,
        commonFactory.cradleConfiguration.cradleMaxMessageBatchSize
    )
    private val dataProvider = commonFactory.grpcRouter.getService(QueueDataProviderService::class.java)

    private val rootEventId: EventID = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }

    private val from = Instant.parse(configuration.from)
    private val to = configuration.to?.run(Instant::parse)
    private val step = Duration.parse(configuration.intervalLength)

    private var currentFrom = from
    private var currentTo = from.doStep()

    init {
        liveness.enable()

        check(to == null || to >= from) {
            "Incorrect configuration parameters: the ${configuration.to} `to` option is less than the ${configuration.from} `from`"
        }

        check(!step.isNegative && !step.isZero) {
            "Incorrect configuration parameters: the ${configuration.intervalLength} `interval length` option is negative or zero"
        }

        check(configuration.awaitTimeout > 0) {
            "Incorrect configuration parameters: the ${configuration.awaitTimeout} `await timeout` option isn't positive"
        }

        check(!configuration.enableStoreState || configuration.stateSessionAlias.isNotBlank()) {
            "Incorrect configuration parameters: the ${configuration.stateSessionAlias} `state session alias` option is blank, " +
                    "the `enable store state` is ${configuration.enableStoreState}"
        }

        check((configuration.messages != null) xor (configuration.events != null)) {
            "Incorrect configuration parameters: " +
                    "both or neither of ${configuration.messages} `messages`, ${configuration.events} `events` options are filled. " +
                    "Processor can work in one mode only"
        }

        if (configuration.messages != null && configuration.events != null) {
            error("Incorrect configuration parameters: " +
                    "Both of `messages`, `events` options are filled. " +
                    "Processor can work in one mode only")
        }
    }

    fun run() {
        val processorEventId = Event.start()
            .name("Processor started ${Instant.now()}")
            .type("Processor start")
            .toBatchProto(rootEventId)
            .also(eventRouter::sendAll)
            .run { getEvents(0).id }

        try {
            val crawlerState = recoverState()
            createProcessor(configuration, processorEventId, crawlerState.processorState).use { processor ->
                val context = Context(
                    eventBatcher,
                    processorEventId,
                    eventRouter,
                    messageRouter,
                    dataProvider,
                    configuration,
                    processor
                )

                when {
                    configuration.messages != null -> GroupMessageCrawler(context)
                    configuration.events != null -> EventCrawler(context)
                    else -> error(
                        "Neither of `messages`, `events` options are filled. " +
                                "Processor must work in any mode"
                    )
                }.use { crawler ->
                    K_LOGGER.info { "Processing started" }
                    readiness.enable()

                    do {
                        val intervalEventId = reportStartProcessing(processorEventId)
                        crawler.processInterval(currentFrom.toTimestamp(), currentTo.toTimestamp(), intervalEventId)
                        storeState(intervalEventId, CrawlerState(currentTo, crawler.serializeState()))
                        reportEndProcessing(intervalEventId)

                        currentFrom = currentTo
                        currentTo = currentFrom.doStep()
                        if (currentFrom == currentTo) {
                            K_LOGGER.info { "Processing completed" }
                            break
                        }
                    } while (true)
                }
            }
        } finally {
            readiness.disable()
        }
    }

    private fun reportStartProcessing(processorEventId: EventID) = Event.start()
            .name("Process interval [$currentFrom - $currentTo)")
            .type(EVENT_TYPE_PROCESS_INTERVAL)
            .toBatchProto(processorEventId)
            .also(eventRouter::sendAll)
            .run { getEvents(0).id }

    private fun reportEndProcessing(intervalEventId: EventID) = Event.start()
        .name("Complete processing")
        .type(EVENT_TYPE_PROCESS_INTERVAL)
        .toBatchProto(intervalEventId)
        .also(eventRouter::sendAll)
        .run { getEvents(0).id }

    private fun storeState(intervalEventId: EventID, crawlerState: CrawlerState) {
        if (configuration.enableStoreState) {
            //TODO:
            K_LOGGER.warn { "Store state method isn't implemented" }
        }
    }

    private fun recoverState(): CrawlerState {
        if (configuration.enableStoreState) {
            //TODO:
            K_LOGGER.warn { "Recover state method isn't implemented" }
        }
        return CrawlerState(Instant.MIN, null)
    }
    private fun createProcessor(
        configuration: Configuration,
        processorEventId: EventID,
        state: ByteArray?
    ): IProcessor = runCatching {
            load<IProcessorFactory>()
        }.getOrElse {
            throw IllegalStateException("Failed to load processor factory", it)
        }.runCatching {
            create(commonFactory, eventBatcher, processorEventId, configuration.processorSettings, state)
        }.getOrElse {
            throw IllegalStateException("Failed to create processor instance", it)
        }

    private fun Instant.doStep(): Instant {
        if (to == this) {
            return this
        }

        val next = this.plus(step)
        return when {
            to != null && to < next -> to
            else -> next
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private const val EVENT_TYPE_PROCESS_INTERVAL = "Process interval"
    }
}



