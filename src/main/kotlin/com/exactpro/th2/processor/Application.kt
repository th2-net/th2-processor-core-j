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
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.metrics.registerLiveness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.utils.state.StateManager
import com.exactpro.th2.dataprovider.lw.grpc.*
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.state.DataProviderStateManager
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
import java.time.Instant
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
    private val dataProvider = commonFactory.grpcRouter.getService(DataProviderService::class.java) // FIXME: possibly needs to be moved somewhere
    private val rootEventId: EventID = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }

    private val processorFactory: IProcessorFactory
    private val configuration: Configuration
    private val stateManager: StateManager
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
            { event -> eventBatcher.onEvent(event) },
            { timestamp -> loadRawMessages(timestamp) },
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
                stateManager,
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

    private fun storeRawMessages(batch: MessageGroupBatch) {
        messageRouter.sendAll(batch)
    }

    private fun loadRawMessages(timestamp: Timestamp): Iterator<MessageSearchResponse> {
        return dataProvider.searchMessages(createSearchRequest(timestamp).also {
            K_LOGGER.info { "Request to load state ${it.toJson()}" }
        })
    }

    private fun createSearchRequest(timestamp: Timestamp = Instant.now().toTimestamp()): MessageSearchRequest =
        requestBuilder().apply {
            startTimestamp = timestamp
        }.build()

    private fun requestBuilder() = MessageSearchRequest.newBuilder().apply {
        searchDirection = TimeRelation.PREVIOUS
        resultCountLimit = Int32Value.of(COUNT_LIMIT)
        addResponseFormats(RAW_MESSAGE_RESPONSE_FORMAT)
        bookIdBuilder.apply {
            name = configuration.bookName
        }
        addStreamBuilder().apply {
            name = configuration.stateSessionAlias
            direction = Direction.SECOND
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