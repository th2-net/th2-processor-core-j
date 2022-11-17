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
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.configuration.DataType.*
import com.exactpro.th2.processor.core.message.MessageCrawler
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

fun main(args: Array<String>) {
    ProcessorCommand(args).run()
}

class ProcessorCommand(args: Array<String>) {
    private val resources: Deque<() -> Unit> = ConcurrentLinkedDeque()

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
    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
    @Suppress("SpellCheckingInspection")
    private val eventBatcher = EventBatcher(executor = scheduler, onBatch = eventRouter::sendAll).apply {
        K_LOGGER.info { "Close event batcher" }
        close()
    }
    private val messageRouter = commonFactory.messageRouterMessageGroupBatch
    private val dataProvider = commonFactory.grpcRouter.getService(DataProviderService::class.java)

    private val rootEventId: EventID = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }.run { EventID.newBuilder().setId(this).build() }

    private val from = Instant.parse(configuration.from)
    private val to = configuration.to?.run(Instant::parse)
    private val step = Duration.parse(configuration.intervalLength)

    private var currentFrom = Instant.MIN
    private var currentTo = Instant.MAX

    init {
        liveness.enable()
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

        check(to == null || to >= from) {
            "Incorrect configuration parameters: the ${configuration.to} `to` option is less than the ${configuration.from} `from`"
        }

        check(!step.isNegative && !step.isZero) {
            "Incorrect configuration parameters: the ${configuration.intervalLength} `interval length` option is negative or zero"
        }

        check(configuration.awaitTimeout > 0) {
            "Incorrect configuration parameters: the ${configuration.awaitTimeout} `await timeout` option isn't positive"
        }
    }

    fun run() {
        try {
            when(configuration.type) {
                MESSAGE_GROUP -> processMessages()
                EVENT -> processEvents()
            }
        } catch (e: InterruptedException) {
            K_LOGGER.error(e) { "Message handling interrupted" }
        } catch (e: Throwable) {
            K_LOGGER.error(e) { "fatal error. Exit the program" }
            exitProcess(1)
        }
    }

    private fun processEvents() {
        TODO("Not yet implemented")
    }

    private fun processMessages() {
        try {
            val processorEventId = Event.start()
                .name("Message processor started ${Instant.now()}")
                .type("Message processor start")
                .toBatchProto(rootEventId)
                .also(eventRouter::sendAll)
                .run { getEvents(0).id }

            val state: ByteArray? = recoverState()

            MessageCrawler(
                messageRouter,
                dataProvider,
                configuration,
                createProcessor(configuration, processorEventId, state)
            ).use { crawler ->
                K_LOGGER.info { "Processing started" }
                readiness.enable()

                while (true) {
                    currentFrom = if (currentTo == Instant.MAX) from else currentTo
                    currentTo = currentFrom.doStep()
                    if (currentFrom == currentTo) {
                        K_LOGGER.info { "Processing completed" }
                        break
                    }

                    crawler.process(currentFrom, currentTo)
                    storeState(crawler.serializeState())
                }
            }
        } finally {
            readiness.disable()
        }
    }

    private fun storeState(serializeState: ByteArray) {
        //TODO:
        K_LOGGER.warn { "Store state method isn't implemented" }
    }

    private fun recoverState(): ByteArray? {
        K_LOGGER.warn { "Recover state method isn't implemented" }
        return null
    }
    private fun createProcessor(
        configuration: Configuration,
        processorEventId: EventID,
        state: ByteArray?
    ): IProcessor = runCatching {
            load<IProcessorFactory>()
        }.getOrElse {
            throw IllegalStateException("Failed to load processor factory", it)
        }.run {
            runCatching {
                create(eventBatcher, processorEventId, configuration.IProcessorSettings, state)
            }.getOrElse {
                throw IllegalStateException("Failed to create processor instance", it)
            }
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
    }
}



