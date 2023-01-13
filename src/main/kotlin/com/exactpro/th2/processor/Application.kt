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

package com.exactpro.th2.processor

import com.exactpro.th2.Service
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.metrics.registerLiveness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.state.StateManager
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.state.DataProviderStateStorage
import com.exactpro.th2.processor.core.state.IStateStorage
import com.exactpro.th2.processor.strategy.AbstractStrategy
import com.exactpro.th2.processor.strategy.CrawlerStrategy
import com.exactpro.th2.processor.strategy.RealtimeStrategy
import com.exactpro.th2.processor.utility.load
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.google.protobuf.Int32Value
import com.google.protobuf.Timestamp
import mu.KotlinLogging
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class Application(
    private val commonFactory: CommonFactory
) : Service {
    private val scheduler = Executors.newScheduledThreadPool(
        1,
        ThreadFactoryBuilder().setNameFormat("processor-core-%d").build()
    )

    @Suppress("SpellCheckingInspection")
    private val liveness = registerLiveness("main")

    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
    private val eventBatcher = EventBatcher(executor = scheduler, onBatch = eventRouter::sendAll)
    private val messageRouter = commonFactory.messageRouterMessageGroupBatch
    private val rootEventId: EventID = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }

    private val processorFactory: IProcessorFactory
    private val configuration: Configuration
    private val stateManager: StateManager

    private val configuration: Configuration
    private val strategy: AbstractStrategy

    init {
        liveness.enable()

        val objectMapper = ObjectMapper(YAMLFactory()).apply {
            registerKotlinModule()
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        }
        processorFactory = load<IProcessorFactory>().apply {
            registerModules(objectMapper)
        }

        with(
            commonFactory
            .getCustomConfiguration(Configuration::class.java, objectMapper)
            .validate()
        ) {
            configuration = this

        stateManager = DataProviderStateManager(
            ::onEvent,
            { bookName, sessionAlias, timestamp -> loadRawMessages(bookName, sessionAlias, timestamp) },
            { statePart -> storeRawMessages(statePart) },
            configuration.stateSessionAlias,
            300000 // TODO: move it to the config
        )

            val processorEventId = processorFactory.createProcessorEvent()
                .toBatchProto(rootEventId)
                .also(eventRouter::sendAll)
                .run { getEvents(0).id }

            val context = Context(
                commonFactory,
                processorFactory,
                processorEventId,
                stateStorage,
                eventBatcher,
                scheduler,
                configuration,
            )

            strategy = when {
                crawler != null -> CrawlerStrategy(context)
                realtime != null -> RealtimeStrategy(context)
                else -> error("$CONFIGURATION_ERROR_PREFIX processor work mode is unknown")
            }
        }
    }

    override fun run() {
        strategy.run()
    }

    override fun close() {
        runCatching {
            K_LOGGER.info { "Close ${strategy::class.java.simpleName}" }
            strategy.close()
        }.onFailure { e ->
            K_LOGGER.error(e) { "Close ${strategy::class.java.simpleName} failure" }
        }

        runCatching {
            K_LOGGER.info { "Close event batcher" }
            eventBatcher.close()
        }.onFailure { e ->
            K_LOGGER.error(e) { "Close event batcher failure" }
        }

        runCatching {
            K_LOGGER.info { "Shutdown scheduler" }
            scheduler.shutdown()
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                K_LOGGER.warn { "Shutdown scheduler failure after 1 second" }
                scheduler.shutdownNow()
            }
        }.onFailure { e ->
            K_LOGGER.error(e) { "Close event batcher failure" }
        }
    }

    private fun doStepAndCheck(processorEventId: EventID, from: Instant): Boolean {
        currentFrom = from
        currentTo = currentFrom.doStep()
        if (currentFrom == currentTo) {
            reportProcessingComplete(processorEventId)
            return false
        }
        return true
    }

    private fun reportProcessingComplete(processorEventId: EventID) = Event.start()
        .name("Whole time range is processed [${configuration.from} - ${configuration.to})")
        .type(EVENT_TYPE_PROCESS_INTERVAL)
        .toBatchProto(processorEventId)
        .log(K_LOGGER)
        .also(eventRouter::sendAll)

    private fun reportStartProcessing(processorEventId: EventID) = Event.start()
        .name("Process interval [$currentFrom - $currentTo)")
        .type(EVENT_TYPE_PROCESS_INTERVAL)
        .toBatchProto(processorEventId)
        .log(K_LOGGER)
        .also(eventRouter::sendAll)
        .run { getEvents(0).id }

    private fun reportEndProcessing(intervalEventId: EventID) = Event.start()
        .name("Complete processing")
        .type(EVENT_TYPE_PROCESS_INTERVAL)
        .toBatchProto(intervalEventId)
        .log(K_LOGGER)
        .also(eventRouter::sendAll)
        .run { getEvents(0).id }

    private fun storeState(processorEventId: EventID, crawlerState: CrawlerState) {
        if (configuration.enableStoreState) {
            OBJECT_MAPPER.writeValueAsBytes(crawlerState).also { rawData ->
                stateStorage.saveState(processorEventId, rawData)
            }
            K_LOGGER.warn { "Store state method isn't implemented" }
        }
    }

    private fun recoverState(processorEventId: EventID): CrawlerState? {
        if (configuration.enableStoreState) {
            stateStorage.loadState(processorEventId)?.let { rawData ->
                runCatching {
                    OBJECT_MAPPER.readValue(rawData, CrawlerState::class.java)
                }.onFailure { e ->
                    throw IllegalStateException("State can't be decode from ${
                        rawData.joinToString("") {
                            it.toString(radix = 16).padStart(2, '0')
                        }
                    }", e
                    )
                }.getOrThrow()
            }
        }
        return null
    }

    private fun createProcessor(
        processorEventId: EventID,
        configuration: Configuration,
        processorState: ByteArray?
    ): IProcessor = runCatching {
        processorFactory.create(
            ProcessorContext(
                commonFactory,
                scheduler,
                eventBatcher,
                processorEventId,
                configuration.processorSettings,
                processorState
            )
        )
    }.getOrElse {
        throw IllegalStateException("Failed to create processor instance", it)
    }

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

        const val EVENT_TYPE_PROCESS_INTERVAL = "Process interval"

        const val CONFIGURATION_ERROR_PREFIX = "Incorrect configuration parameters: "

        private const val RAW_MESSAGE_RESPONSE_FORMAT = "BASE_64"
        private const val COUNT_LIMIT = 100

        val DIRECTION = Direction.SECOND

        private fun Configuration.validate(): Configuration = this.apply {
            check((crawler != null) xor (realtime != null)) {
                "Incorrect configuration parameters: process must work in one of crawler (configured: ${crawler != null}) / realtime (configured: ${realtime != null}) mode."
            }

            check(!enableStoreState || stateSessionAlias.isNotBlank()) {
                "Incorrect configuration parameters: the $stateSessionAlias `state session alias` option is blank, " +
                        "the `enable store state` is $enableStoreState"
            }
        }
    }
}