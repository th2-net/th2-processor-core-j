/*
 *  Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.metrics.registerLiveness
import com.exactpro.th2.common.metrics.registerReadiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.add
import com.exactpro.th2.dataprovider.grpc.*
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.configuration.DataType.*
import com.exactpro.th2.processor.core.message.MessageCrawler
import com.exactpro.th2.processor.core.state.manager.MessageStateManager
import com.exactpro.th2.processor.utility.load
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
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

        val commonFactory = CommonFactory.createFromArguments(*args).apply {
            resources.add {
                K_LOGGER.info { "Closing common factory" }
                close()
            }
        }

        Application(commonFactory)
            .use(Application::run)
    } catch (e: InterruptedException) {
        K_LOGGER.error(e) { "Message handling interrupted" }
    } catch (e: Throwable) {
        K_LOGGER.error(e) { "fatal error. Exit the program" }
        e.printStackTrace()
        exitProcess(1)
    }
}

class ProcessorCommand(
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
    private val processorEventId = when(configuration.type) {
        MESSAGE_GROUP -> createRootEvent("Message")
        EVENT -> createRootEvent("Event")
    }
    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
    private val eventBatcher = EventBatcher(executor = scheduler, onBatch = eventRouter::sendAll).apply {
        resources.add {
            K_LOGGER.info { "Close event batcher" }
            close()
        }
    }
    private val messageRouter = commonFactory.messageRouterMessageGroupBatch
    private val dataProvider = commonFactory.grpcRouter.getService(DataProviderService::class.java)

    private val rootEventId: EventID = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }.run { EventID.newBuilder().setId(this).build() }

    private val from = Instant.parse(configuration.from)
    private val to = configuration.to?.run(Instant::parse)
    private val step = Duration.parse(configuration.intervalLength)

    // TODO: move limits to the config
    // TODO: add more params to loadRawMessages func
    private val stateManager = MessageStateManager(
        100,
        eventBatcher::onEvent,
        { bookId, sessionAlias -> dataProvider.searchMessages(
            MessageSearchRequest.newBuilder()
                .setBookId(BookId.newBuilder().setName(bookId))
                .addStream(MessageStream.newBuilder().setName(sessionAlias)).build()
        ) },
        { statePart -> messageRouter.send(MessageGroupBatch.newBuilder().addGroups(MessageGroup.newBuilder().add(statePart)).build()) }
    )


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
    }

    fun run() {
        when(configuration.type) {
            MESSAGE_GROUP -> processMessages()
            EVENT -> processEvents()
        }
    }

    private fun processEvents() {
        TODO("Not yet implemented")
    }

    private fun processMessages() {
        try {
            val state: ByteArray? = recoverState() // FIXME: where to take bookId from?

            MessageCrawler(
                messageRouter,
                dataProvider,
                configuration,
                createProcessor(configuration, processorEventId, state)
            ).use { crawler ->
                K_LOGGER.info { "Processing started" }
                readiness.enable()

                do {
                    crawler.process(currentFrom, currentTo)
                    storeState(crawler.serializeState()) // FIXME: where to take bookId from?

                    currentFrom = currentTo
                    currentTo = currentFrom.doStep()
                    if (currentFrom == currentTo) {
                        K_LOGGER.info { "Processing completed" }
                        break
                    }
                } while (true)
            }
        } finally {
            readiness.disable()
        }
    }

    private fun storeState(serializeState: ByteArray) {
        stateManager.store(serializeState, configuration.stateSessionAlias, configuration.bookId)
    }

    private fun recoverState(): ByteArray? {
        return stateManager.load(configuration.stateSessionAlias, configuration.bookId)
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
                create(eventBatcher, processorEventId, configuration.processorSettings, state)
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

    private fun createRootEvent(dataType: String): EventID {
        return Event.start()
            .name("$dataType processor started ${Instant.now()}")
            .type("$dataType processor start")
            .toBatchProto(rootEventId)
            .also(eventRouter::sendAll)
            .run { getEvents(0).id }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}



