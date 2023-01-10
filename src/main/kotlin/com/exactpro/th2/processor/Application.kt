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
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.api.IProcessorSettings
import com.exactpro.th2.processor.api.ProcessorContext
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.state.CrawlerState
import com.exactpro.th2.processor.core.state.DataProviderStateStorage
import com.exactpro.th2.processor.core.state.IStateStorage
import com.exactpro.th2.processor.strategy.AbstractStrategy
import com.exactpro.th2.processor.strategy.CrawlerStrategy
import com.exactpro.th2.processor.strategy.RealtimeStrategy
import com.exactpro.th2.processor.utility.OBJECT_MAPPER
import com.exactpro.th2.processor.utility.load
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.common.util.concurrent.ThreadFactoryBuilder
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
    private val dataProvider = commonFactory.grpcRouter.getService(QueueDataProviderService::class.java)
    private val rootEventId: EventID = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }

    private val processorFactory: IProcessorFactory
    private val stateStorage: IStateStorage

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

            stateStorage = DataProviderStateStorage(
                messageRouter,
                eventBatcher,
                commonFactory.grpcRouter.getService(DataProviderService::class.java),
                commonFactory.boxConfiguration.bookName,
                stateSessionAlias,
                commonFactory.cradleConfiguration.cradleMaxMessageBatchSize
            )

            strategy = when {
                crawler != null -> CrawlerStrategy(eventRouter, configuration, stateStorage)
                realtime != null -> RealtimeStrategy()
                else -> error("$CONFIGURATION_ERROR_PREFIX processor work mode is unknown")
            }
        }
    }

    override fun run() {
        val processorEventId = processorFactory.createProcessorEvent()
            .toBatchProto(rootEventId)
            .also(eventRouter::sendAll)
            .run { getEvents(0).id }

            val crawlerState: CrawlerState? = recoverState(processorEventId)
            createProcessor(
                processorEventId,
                configuration.processorSettings,
                crawlerState?.processorState
            ).use { processor ->
                val context = Context(
                    eventBatcher,
                    processorEventId,
                    eventRouter,
                    messageRouter,
                    dataProvider,
                    configuration,
                    processor
                )

                strategy.run(context, crawlerState, processor)
            }
    }

    override fun close() {
        runCatching {
            K_LOGGER.info { "Close event batcher" }
            eventBatcher.close()
        }.onFailure { e ->
            K_LOGGER.error(e) { "Close event batcher failure" }
        }

        kotlin.runCatching {
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
        processorSettings: IProcessorSettings,
        processorState: ByteArray?
    ): IProcessor = runCatching {
        processorFactory.create(
            ProcessorContext(
                commonFactory,
                scheduler,
                eventBatcher,
                processorEventId,
                processorSettings,
                processorState
            )
        )
    }.getOrElse {
        throw IllegalStateException("Failed to create processor instance", it)
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        const val EVENT_TYPE_PROCESS_INTERVAL = "Process interval"

        const val CONFIGURATION_ERROR_PREFIX = "Incorrect configuration parameters: "

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