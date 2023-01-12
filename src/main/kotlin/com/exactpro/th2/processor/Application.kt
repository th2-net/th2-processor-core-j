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
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.metrics.registerLiveness
import com.exactpro.th2.common.metrics.registerReadiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.utils.state.StateManager
import com.exactpro.th2.dataprovider.lw.grpc.*
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.api.ProcessorContext
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.Crawler
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.event.EventCrawler
import com.exactpro.th2.processor.core.message.CradleMessageGroupCrawler
import com.exactpro.th2.processor.core.state.CrawlerState
import com.exactpro.th2.processor.core.state.DataProviderStateManager
import com.exactpro.th2.processor.utility.OBJECT_MAPPER
import com.exactpro.th2.processor.utility.load
import com.exactpro.th2.processor.utility.log
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.google.protobuf.Int32Value
import com.google.protobuf.Timestamp
import mu.KotlinLogging
import java.time.Duration
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
    private val readiness = registerReadiness("main")
    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
    private val eventBatcher = EventBatcher(executor = scheduler, onBatch = eventRouter::sendAll)
    private val messageRouter = commonFactory.messageRouterMessageGroupBatch
    private val queueDataProvider = commonFactory.grpcRouter.getService(QueueDataProviderService::class.java)
    private val dataProvider = commonFactory.grpcRouter.getService(DataProviderService::class.java)
    private val rootEventId: EventID = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }

    private val processorFactory: IProcessorFactory
    private val configuration: Configuration
    private val stateManager: StateManager

    private val from: Instant
    private val to: Instant?
    private val intervalLength: Duration

    private var currentFrom: Instant
    private var currentTo: Instant

    init {
        liveness.enable()

        val objectMapper = ObjectMapper(YAMLFactory()).apply {
            registerKotlinModule()
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        }
        processorFactory = load<IProcessorFactory>().apply {
            registerModules(objectMapper)
        }

        configuration = commonFactory
            .getCustomConfiguration(Configuration::class.java, objectMapper)
            .validate()

        stateManager = DataProviderStateManager(
            ::onEvent,
            { bookName, sessionAlias, timestamp -> loadRawMessages(bookName, sessionAlias, timestamp) },
            { statePart -> storeRawMessages(statePart) },
            configuration.stateSessionAlias,
            300000 // TODO: move it to the config
        )

        from = Instant.parse(configuration.from)
        to = configuration.to?.run(Instant::parse)
        check(to == null || to >= from) {
            "Incorrect configuration parameters: " +
                    "the ${configuration.to} `to` option is less than the ${configuration.bookName} `from`"
        }

        intervalLength = Duration.parse(configuration.intervalLength)
        check(!intervalLength.isNegative && !intervalLength.isZero) {
            "Incorrect configuration parameters: " +
                    "the ${configuration.intervalLength} `interval length` option is negative or zero"
        }
        val syncInterval = Duration.parse(configuration.syncInterval)
        check(!syncInterval.isNegative && !syncInterval.isZero) {
            "Incorrect configuration parameters: " +
                    "the ${configuration.syncInterval} `synchronize interval` option is negative or zero"
        }
        check(syncInterval <= intervalLength) {
            "Incorrect configuration parameters: " +
                    "the ${configuration.syncInterval} `synchronize interval` option is greater than the ${configuration.intervalLength} `interval length`"
        }

        currentFrom = from
        currentTo = from.doStep()
    }

    override fun run() {
        val processorEventId = processorFactory.createProcessorEvent()
            .toBatchProto(rootEventId)
            .also(eventRouter::sendAll)
            .run { getEvents(0).id }

        try {
            val crawlerState: CrawlerState? = recoverState(processorEventId)?.also { state ->
                if (!doStepAndCheck(processorEventId, state.timestamp)) {
                    return
                }
            }
            createProcessor(processorEventId, configuration, crawlerState?.processorState).use { processor ->
                val context = Context(
                    eventBatcher,
                    processorEventId,
                    eventRouter,
                    messageRouter,
                    queueDataProvider,
                    configuration,
                    processor
                )

                val crawlers: Set<Crawler<*>> = mutableSetOf<Crawler<*>>().apply {
                    configuration.messages?.let { add(CradleMessageGroupCrawler(context)) }
                    configuration.events?.let { add(EventCrawler(context)) }
                }
                try {
                    check(crawlers.isNotEmpty()) {
                        "Neither of `messages`, `events` options are filled. Processor must work in any mode"
                    }
                    K_LOGGER.info { "Processing started" }
                    readiness.enable()

                    do {
                        val intervalEventId = reportStartProcessing(processorEventId)
                        crawlers.parallelStream().forEach { crawler ->
                            crawler.processInterval(currentFrom.toTimestamp(), currentTo.toTimestamp(), intervalEventId)
                        }
                        storeState(intervalEventId, CrawlerState(currentTo, processor.serializeState()))
                        reportEndProcessing(intervalEventId)

                        if (!doStepAndCheck(processorEventId, currentTo)) {
                            break
                        }
                    } while (true)
                } finally {
                    crawlers.forEach { crawler ->
                        runCatching(crawler::close)
                            .onFailure { e ->
                                K_LOGGER.error(e) { "Closing ${crawler::class.java.simpleName} failure" }
                            }
                    }
                }
            }
        } finally {
            readiness.disable()
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
        .name("Whole time range is processed [${configuration.bookName} - ${configuration.to})")
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
                stateManager.store(processorEventId, rawData, configuration.stateSessionAlias, configuration.bookName)
            }
            K_LOGGER.warn { "Store state method isn't implemented" }
        }
    }

    private fun recoverState(processorEventId: EventID): CrawlerState? {
        if (configuration.enableStoreState) {
            stateManager.load(processorEventId, configuration.stateSessionAlias, configuration.bookName)?.let { rawData ->
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

    private fun storeRawMessages(batch: MessageGroupBatch) {
        messageRouter.send(batch)
    }

    private fun loadRawMessages(bookName: String, sessionAlias: String, timestamp: Timestamp): Iterator<MessageSearchResponse> {
        return dataProvider.searchMessages(createSearchRequest(bookName, sessionAlias, timestamp).also {
            K_LOGGER.info { "Request to load state ${it.toJson()}" }
        })
    }

    private fun createSearchRequest(bookName: String, sessionAlias: String, timestamp: Timestamp = Instant.now().toTimestamp()): MessageSearchRequest =
        requestBuilder.apply {
            startTimestamp = timestamp
            bookId = BookId.newBuilder().setName(bookName).build()
            addStream(MessageStream.newBuilder().setName(sessionAlias).setDirection(DIRECTION).build())
        }.build()

    private val requestBuilder = MessageSearchRequest.newBuilder().apply {
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

    private fun onEvent(event: com.exactpro.th2.common.grpc.Event) {
        eventBatcher.onEvent(event)
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private const val EVENT_TYPE_PROCESS_INTERVAL = "Process interval"

        private const val RAW_MESSAGE_RESPONSE_FORMAT = "BASE_64"
        private const val COUNT_LIMIT = 100

        val DIRECTION = Direction.SECOND

        private fun Configuration.validate(): Configuration = this.apply {
            check(awaitTimeout > 0) {
                "Incorrect configuration parameters: the $awaitTimeout `await timeout` option isn't positive"
            }

            check(!enableStoreState || stateSessionAlias.isNotBlank()) {
                "Incorrect configuration parameters: the $stateSessionAlias `state session alias` option is blank, " +
                        "the `enable store state` is $enableStoreState"
            }

            check((messages != null) || (events != null)) {
                "Incorrect configuration parameters: " +
                        "neither of $messages `messages`, $events `events` options are filled."
            }
        }
    }
}