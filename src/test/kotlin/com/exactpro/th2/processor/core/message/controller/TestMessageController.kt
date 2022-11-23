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

package com.exactpro.th2.processor.core.message.controller

import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.MessageIntervalInfo
import com.exactpro.th2.processor.api.IProcessor
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class TestMessageController {

    private lateinit var processor: IProcessor
    private lateinit var messageController: MessageController

    @BeforeEach
    fun beforeEach() {
        processor = mock {  }
        messageController = MessageController(
            processor,
            TH2_GROUPS,
            INTERVAL_START,
            INTERVAL_END
        )
    }

    @Test
    fun `put empty expected value`() {
        assertDoesNotThrow("Pass empty expected") {
            messageController.expected(MessageIntervalInfo.newBuilder().apply {
                startTimestamp = INTERVAL_START.toTimestamp()
                endTimestamp = INTERVAL_END.toTimestamp()
            }.build())
        }
        assertTrue(messageController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @Test
    fun `put expected value with timestamp out of interval`() {
        assertFailsWith<IllegalStateException>("Start interval timestamp is lower than expected") {
            messageController.expected(MessageIntervalInfo.newBuilder().apply {
                startTimestamp = INTERVAL_START.minusNanos(1).toTimestamp()
                endTimestamp = INTERVAL_END.toTimestamp()
            }.build())
        }

        assertFailsWith<IllegalStateException>("End interval timestamp is greater as expected") {
            messageController.expected(MessageIntervalInfo.newBuilder().apply {
                startTimestamp = INTERVAL_START.toTimestamp()
                endTimestamp = INTERVAL_END.plusNanos(1).toTimestamp()
            }.build())
        }
    }

    @Test
    fun `receive unknown group`() {
        messageController.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += message(UNKNOWN_GROUP, UNKNOWN_SESSION_ALIAS, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
            }
        }.build())

        verify(processor, never()).handle(any())
        assertTrue(messageController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @Test
    fun `receive out of time message`() {
        messageController.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += message(KNOWN_GROUP, KNOWN_SESSION_ALIAS, INTERVAL_START.minusNanos(1))
            }
        }.build())
        verify(processor, never().description("Message before interval start")).handle(any())

        messageController.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += message(KNOWN_GROUP, KNOWN_SESSION_ALIAS, INTERVAL_END)
            }
        }.build())
        verify(processor, never().description("Message with end interval timestamp")).handle(any())

        assertTrue(messageController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @Test
    fun `receive correct messages`() {
        val minMessage = message(KNOWN_GROUP, KNOWN_SESSION_ALIAS, INTERVAL_START)
        messageController.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += minMessage
            }
        }.build())
        verify(processor, times(1).description("Message with start interval timestamp")).handle(eq(minMessage))

        val intermediateMessage = message(KNOWN_GROUP, KNOWN_SESSION_ALIAS, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
        messageController.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += intermediateMessage
            }
        }.build())
        verify(processor, times(1).description("Message with half interval timestamp")).handle(eq(intermediateMessage))

        val maxMessage = message(KNOWN_GROUP, KNOWN_SESSION_ALIAS, INTERVAL_END.minusNanos(1))
        messageController.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += maxMessage
            }
        }.build())
        verify(processor, times(1).description("Message the nearest to the end interval timestamp")).handle(eq(maxMessage))

        assertFalse(messageController.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
        messageController.expected(MessageIntervalInfo.newBuilder().apply {
            startTimestamp = INTERVAL_START.toTimestamp()
            endTimestamp = INTERVAL_END.toTimestamp()

            addMessagesInfoBuilder().apply {
                sessionAlias = KNOWN_SESSION_ALIAS
                numberOfMessages = 3
                //TODO: fill max/min sequences and timestamps
            }
        }.build())
        assertTrue(messageController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @Test
    fun `multiple expected method calls`() {
        val cycles = 2
        repeat(cycles) {
            messageController.actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    this += message(KNOWN_GROUP, KNOWN_SESSION_ALIAS, INTERVAL_START)
                }
            }.build())
        }

        verify(processor, times(cycles).description("Handled messages")).handle(any())

        repeat(cycles) {
            assertFalse(messageController.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
            messageController.expected(MessageIntervalInfo.newBuilder().apply {
                startTimestamp = INTERVAL_START.toTimestamp()
                endTimestamp = INTERVAL_END.toTimestamp()

                addMessagesInfoBuilder().apply {
                    sessionAlias = KNOWN_SESSION_ALIAS
                    numberOfMessages = 1
                    //TODO: fill max/min sequences and timestamps
                }
            }.build())
        }
        assertTrue(messageController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    private fun message(group: String, sessionAlias: String, timestamp: Instant): Message = Message.newBuilder().apply {
        metadataBuilder.apply {
            this.timestamp = timestamp.toTimestamp()
            idBuilder.apply {
                connectionIdBuilder.apply {
                    this.sessionGroup = group
                    this.sessionAlias = sessionAlias
                }
            }
        }
    }.build()

    companion object {
        private val INTERVAL_HALF_LENGTH = Duration.ofMinutes(1)
        private val INTERVAL_LENGTH = INTERVAL_HALF_LENGTH + INTERVAL_HALF_LENGTH
        private val INTERVAL_START = Instant.now()
        private val INTERVAL_END = INTERVAL_START.plus(INTERVAL_LENGTH)

        private const val KNOWN_SESSION_ALIAS = "known-session-alias"
        private const val UNKNOWN_SESSION_ALIAS = "unknown-session-alias"
        private const val KNOWN_GROUP = "known-group"
        private const val UNKNOWN_GROUP = "unknown-group"
        private val TH2_GROUPS = setOf(KNOWN_GROUP)
    }
}