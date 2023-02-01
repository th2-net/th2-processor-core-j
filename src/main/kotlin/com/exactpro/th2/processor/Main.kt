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

import com.exactpro.th2.common.schema.factory.CommonFactory
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

        Application(commonFactory).apply {
            resources.add {
                K_LOGGER.info { "Closing application" }
                close()
            }
            run()
        }
    } catch (e: InterruptedException) {
        K_LOGGER.error(e) { "Message handling interrupted" }
    } catch (e: Throwable) {
        K_LOGGER.error(e) { "fatal error. Exit the program" }
        e.printStackTrace()
        exitProcess(1)
    }
}