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

import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.metrics.registerLiveness
import com.exactpro.th2.common.metrics.registerReadiness
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.configuration.DataType.*
import com.exactpro.th2.processor.core.message.MessageCrawler
import com.exactpro.th2.processor.utility.load
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import mu.KotlinLogging
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import kotlin.concurrent.thread
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    ProcessorCommand().main(args)
}

class ProcessorCommand : CliktCommand() {
    private val configs: String? by option(help = "Directory containing schema files")
    private val resources: Deque<() -> Unit> = ConcurrentLinkedDeque()

    private val commonFactory: CommonFactory = when (configs) {
        null -> CommonFactory()
        else -> CommonFactory.createFromArguments("--configs=$configs")
    }.apply {
        resources.add {
            K_LOGGER.info("Closing common factory")
            close()
            liveness.disable()
        }
    }

    @Suppress("SpellCheckingInspection")
    private val liveness = registerLiveness("main")
    private val readiness = registerReadiness("main")
    private val configuration = Configuration.create(commonFactory)
    private val eventRouter: MessageRouter<EventBatch> = commonFactory.eventBatchRouter
    private val messageRouter = commonFactory.messageRouterMessageGroupBatch
    private val dataProvider = commonFactory.grpcRouter.getService(DataProviderService::class.java)

    private val rootEventId: String = requireNotNull(commonFactory.rootEventId) {
        "Common's root event id can not be null"
    }

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
    }

    override fun run() {
        runCatching {
            when(configuration.type) {
                MESSAGE -> processMessages()
                EVENT -> processEvents()
            }
        }.onFailure {exception ->
            K_LOGGER.error(exception) { "fatal error. Exit the program" }
            exitProcess(1)
        }
    }

    private fun processEvents() {
        TODO("Not yet implemented")
    }

    private fun processMessages() {
        val state: ByteArray? = recoverMessageState()

        try {
            MessageCrawler(
                messageRouter,
                dataProvider,
                configuration,
                create(configuration, state)
            ).use { crawler ->
                K_LOGGER.info { "Processing started" }
                readiness.enable()

                while (crawler.process()) {
                    storeMessageState(crawler.serializeState())
                }

                K_LOGGER.info { "Processing completed" }
            }
        } finally {
            readiness.disable()
        }
    }

    private fun storeMessageState(serializeState: ByteArray) {
        //TODO:
    }

    private fun recoverMessageState(): ByteArray? {
        //TODO("Not yet implemented")
        return null
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        fun create(configuration: Configuration, state: ByteArray?): IProcessor {
            val factory = runCatching {
                load<IProcessorFactory>()
            }.getOrElse {
                throw IllegalStateException("Failed to load processor factory", it)
            }

            return configuration.IProcessorSettings.runCatching {
                factory.create(this, state)
            }.getOrElse {
                throw IllegalStateException("Failed to create processor instance", it)
            }
        }
    }
}



