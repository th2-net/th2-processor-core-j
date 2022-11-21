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

package com.exactpro.th2.processor.core.message.controller

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.direction
import com.exactpro.th2.common.message.logId
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.sessionGroup
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.MessageIntervalInfo
import com.exactpro.th2.processor.core.message.StreamKey
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.util.Timestamps
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils
import java.time.Instant
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import javax.annotation.concurrent.ThreadSafe
import kotlin.concurrent.withLock

@ThreadSafe
class MessageController(
    private val th2Groups: Set<String>,
    private val startTime: Instant,
    private val endTime: Instant
) : IMessageController {
    private val state = ConcurrentHashMap<StreamKey, Long>()
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    private val startTimestamp = startTime.toTimestamp()
    private val endTimestamp = endTime.toTimestamp()

    override fun actual(batch: MessageGroupBatch) {
        var needCheck = false

        val directionMap = EnumMap<Direction, MutableMap<String, Long>>(Direction::class.java)
        for (group in batch.groupsList) {
            for(anyMessage in group.messagesList) {
                check(anyMessage.hasMessage()) {
                    "${anyMessage.logId} message is not th2 parsed message"
                }

                val message = anyMessage.message
                val timestamp = message.metadata.timestamp
                if (!th2Groups.contains(message.sessionGroup)
                    || Timestamps.compare(timestamp, startTimestamp) < 0
                    || Timestamps.compare(timestamp, endTimestamp) > 0) {
                    K_LOGGER.warn { "unexpected message ${message.shortId()}, timestamp ${shortDebugString(timestamp)}" }
                    continue
                }

                val direction = message.direction
                when {
                    direction.number >= 0 -> directionMap.getOrPut(direction, ::hashMapOf)
                        .compute(message.sessionAlias) { _, current -> (current ?: 0L) + 1 }
                    else -> error("Unknown $direction direction in the ${message.logId} message")
                }
            }
        }

        directionMap.forEach { (direction, aliasMap) ->
            aliasMap.forEach { (alias, count) ->
                val streamKey = StreamKey(alias, direction)

                state.compute(streamKey) { _, previous ->
                    when (val result = (previous ?: 0L) + count) {
                        0L -> {
                            needCheck = true
                            null
                        }

                        else -> result
                    }
                }
            }
        }

        K_LOGGER.debug { "Actual data has received, need check = $needCheck, state = $state" }
        if (needCheck) {
            verify()
        }
    }

    override fun expected(intervalInfo: MessageIntervalInfo) {
        var needCheck = false
        intervalInfo.messagesInfoList.asSequence()
            .filterNot { StringUtils.isBlank(it.sessionAlias) }
            .forEach { streamInfo ->
                state.compute(StreamKey(streamInfo.sessionAlias, streamInfo.direction)) { _, previous ->
                    when (val result = (previous ?: 0L) - streamInfo.numberOfMessages) {
                        0L -> {
                            needCheck = true
                            null
                        }

                        else -> result
                    }
                }
            }

        K_LOGGER.debug { "Expected data has received, need check = $needCheck, state = $state" }
        if (needCheck) {
            verify()
        }
    }

    override fun await(time: Long, unit: TimeUnit): Boolean = lock.withLock {
        if (condition.await(time, unit)) {
            state.isEmpty()
        }
        false
    }

    override fun toString(): String {
        return "groups: $th2Groups, interval: [$startTime, $endTime), state $state"
    }

    private fun verify() {
        if (state.isEmpty()) {
            lock.withLock { condition.signalAll() }
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        fun Message.shortId() = "$sessionGroup:$sessionAlias:$direction:$sequence"
    }
}

