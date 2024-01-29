/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.processor.core

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

abstract class Controller<T>(
    val intervalEventId: EventID
) {
    /**
     * Controller updates this marker on each actual processed message which passed precondition
     */
    @Volatile
    private var lastProcessedTimestamp: Instant = Instant.MIN
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()
    @Volatile // the actual method won't be call when collector expected zero incoming messages
    private var receivedExpected = false

    open val isStateComplete: Boolean
        get() = receivedExpected

    abstract fun actual(batch: T)
    open fun expected(loadedStatistic: MessageLoadedStatistic) {
        receivedExpected = true
    }
    open fun expected(loadedStatistic: EventLoadedStatistic) {
        receivedExpected = true
    }
    /**
     * Wait complete state
     */
    fun await(time: Long, unit: TimeUnit): Boolean {
        if (isStateComplete) {
            return true
        }

        lock.withLock {
            val actualTimestampGenerator = sequence {
                while (true) {
                    yield(lastProcessedTimestamp)
                }
            }.iterator()
            var counter = 0

            while (true) {
                counter++
                val previous = actualTimestampGenerator.next()
                if (condition.await(time, unit) || previous === actualTimestampGenerator.next()) {
                    break
                } else {
                    K_LOGGER.info {
                        "Controller has been processing actual messages, " +
                                "previous timestamp $previous, " +
                                "waiting attempt $counter for $time $unit"
                    }
                }
            }
        }

        return isStateComplete
    }
    protected fun updateLastProcessed(timestamp: Instant) {
        lastProcessedTimestamp = timestamp
    }
    protected fun signal() = lock.withLock {
        condition.signalAll()
    }
    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}