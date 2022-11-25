/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.LoadedStatistic
import com.google.protobuf.Message
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import mu.KotlinLogging
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

abstract class Controller<T: Message> {
    /**
     * Controller updates this marker on each actual processed message which passed precondition
     */
    @Volatile
    private var lastProcessedTimestamp: Timestamp = Timestamps.MIN_VALUE
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    abstract val isStateEmpty: Boolean
    abstract fun actual(batch: T)
    open fun expected(loadedStatistic: LoadedStatistic) {
        throw UnsupportedOperationException()
    }
    open fun expected(loadedStatistic: EventLoadedStatistic) {
        throw UnsupportedOperationException()
    }
    fun await(time: Long, unit: TimeUnit): Boolean {
        if (isStateEmpty) {
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
                                "previous timestamp ${Timestamps.toString(previous)}, " +
                                "waiting attempt $counter for $time $unit"
                    }
                }
            }
        }

        return isStateEmpty
    }
    protected fun updateLastProcessed(timestamp: Timestamp) {
        lastProcessedTimestamp = timestamp
    }
    protected fun signal() = lock.withLock {
        condition.signalAll()
    }
    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}