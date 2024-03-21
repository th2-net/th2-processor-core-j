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
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.metrics.registerLiveness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.state.DummyStateStorage
import com.exactpro.th2.processor.core.state.IStateStorage
import com.exactpro.th2.processor.strategy.AbstractStrategy
import com.exactpro.th2.processor.utility.load
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.common.util.concurrent.ThreadFactoryBuilder
import mu.KotlinLogging
import java.time.Clock
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import com.exactpro.th2.processor.core.state.protobuf.DataProviderStateStorage as ProtobufDataProviderStateStorage
import com.exactpro.th2.processor.core.state.transport.DataProviderStateStorage as TransportDataProviderStateStorage
import com.exactpro.th2.processor.strategy.protobuf.CrawlerStrategy as ProtobufCrawlerStrategy
import com.exactpro.th2.processor.strategy.protobuf.RealtimeStrategy as ProtobufRealtimeStrategy
import com.exactpro.th2.processor.strategy.transport.CrawlerStrategy as TransportCrawlerStrategy
import com.exactpro.th2.processor.strategy.transport.RealtimeStrategy as TransportRealtimeStrategy

class Application internal constructor(
    private val commonFactory: CommonFactory,
    private val timeSource: Clock
) : Service {
    constructor(commonFactory: CommonFactory) : this(commonFactory, Clock.systemUTC())

    private val scheduler = Executors.newScheduledThreadPool(
        1,
        ThreadFactoryBuilder().setNameFormat("processor-core-%d").build()
    )

    private val liveness = registerLiveness("main")

    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
    private val eventBatcher = EventBatcher(executor = scheduler, onBatch = eventRouter::sendAll)
    private val rootEventId: EventID = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }

    private val processorFactory: IProcessorFactory
    private val stateStorage: IStateStorage

    private val strategy: AbstractStrategy

    init {
        liveness.enable()

        val objectMapper = ObjectMapper(YAMLFactory()).apply {
            registerKotlinModule()
            registerModule(JavaTimeModule())
        }
        processorFactory = load<IProcessorFactory>().apply {
            registerModules(objectMapper)
        }

        with(
            commonFactory
            .getCustomConfiguration(Configuration::class.java, objectMapper)
        ) {
            val maxMessageSize = commonFactory.cradleManager.storage.entitiesFactory.maxMessageBatchSize.toLong()

            stateStorage = if (enableStoreState) {
                if (useTransport) {
                    TransportDataProviderStateStorage(
                        commonFactory.transportGroupBatchRouter,
                        eventBatcher,
                        commonFactory.grpcRouter.getService(DataProviderService::class.java),
                        commonFactory.boxConfiguration.bookName,
                        requireNotNull(stateSessionAlias) { "`state session alias` can be empty" },
                        maxMessageSize,
                    )
                } else {
                    ProtobufDataProviderStateStorage(
                        commonFactory.messageRouterMessageGroupBatch,
                        eventBatcher,
                        commonFactory.grpcRouter.getService(DataProviderService::class.java),
                        commonFactory.boxConfiguration.bookName,
                        requireNotNull(stateSessionAlias) { "`state session alias` can be empty" },
                        maxMessageSize,
                    )
                }
            } else {
                 DummyStateStorage()
            }

            val processorEventId: EventID = processorFactory.createProcessorEvent()
                .toBatchProto(rootEventId)
                .also(eventRouter::sendAll)
                .run {
                    check(eventsCount == 1) {
                        "The ${processorFactory::class.simpleName} produce complex event for processor instead of single ${this.toJson(true)}"
                    }
                    getEvents(0).id
                }

            val context = Context(
                commonFactory,
                processorFactory,
                processorEventId,
                stateStorage,
                eventBatcher,
                scheduler,
                configuration = this,
                timeSource = timeSource,
            )

            strategy = when {
                crawler != null -> if (useTransport) {
                    TransportCrawlerStrategy(context)
                } else {
                    ProtobufCrawlerStrategy(context)
                }
                realtime != null -> if (useTransport) {
                    TransportRealtimeStrategy(context)
                } else {
                    ProtobufRealtimeStrategy(context)
                }
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

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        const val EVENT_TYPE_PROCESS_INTERVAL = "Process interval"

        const val CONFIGURATION_ERROR_PREFIX = "Incorrect configuration parameters: "
    }
}