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

import com.exactpro.th2.common.grpc.AnyMessage.KindCase
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.message.CrawlerHandleMessageException
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.verification.VerificationMode
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class TestGroupController {

    private val processor: IProcessor = mock {  }
    private val messageController: MessageController = GroupController(
        processor,
        INTERVAL_EVENT_ID,
        INTERVAL_START.toTimestamp(),
        INTERVAL_END.toTimestamp(),
        KindCase.MESSAGE,
        BOOK_TO_GROUPS
    )
    private val rawMessageController: MessageController = GroupController(
        processor,
        INTERVAL_EVENT_ID,
        INTERVAL_START.toTimestamp(),
        INTERVAL_END.toTimestamp(),
        KindCase.RAW_MESSAGE,
        BOOK_TO_GROUPS
    )

    @ParameterizedTest
    @MethodSource("kinds")
    fun `put event expected value`(kind: KindCase) {
        assertFailsWith<UnsupportedOperationException>("Call unsupported expected overload") {
            getController(kind).expected(EventLoadedStatistic.getDefaultInstance())
        }
    }
    @ParameterizedTest
    @MethodSource("kinds")
    fun `put empty expected value`(kind: KindCase) {
        val controller = getController(kind)
        assertDoesNotThrow("Pass empty expected") {
            controller.expected(MessageLoadedStatistic.getDefaultInstance())
        }
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("kinds")
    fun `put unknown book expected value`(kind: KindCase) {
        val controller = getController(kind)
        assertFailsWith<IllegalStateException>("Check statistic unknown book") {
            controller.expected(MessageLoadedStatistic.newBuilder().apply {
                addStatBuilder().apply {
                    bookIdBuilder.apply { name = UNKNOWN_BOOK }
                    groupBuilder.apply { name = KNOWN_GROUP }
                }
            }.build())
        }
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("kinds")
    fun `put unknown group expected value`(kind: KindCase) {
        val controller = getController(kind)
        assertFailsWith<IllegalStateException>("Check statistic with unknown group") {
            controller.expected(MessageLoadedStatistic.newBuilder().apply {
                addStatBuilder().apply {
                    bookIdBuilder.apply { name = KNOWN_BOOK }
                    groupBuilder.apply { name = UNKNOWN_GROUP }
                }
            }.build())
        }
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("kinds")
    fun `receive unknown book`(kind: KindCase) {
        val controller = getController(kind)
        assertFailsWith<CrawlerHandleMessageException>("Check message with unknown book") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(kind, UNKNOWN_BOOK, KNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
                }
            }.build())
        }

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
        ))
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("kinds")
    fun `receive unknown group`(kind: KindCase) {
        val controller = getController(kind)
        assertFailsWith<CrawlerHandleMessageException>("Check message with unknown group") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(kind, KNOWN_BOOK, UNKNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
                }
            }.build())
        }

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
        ))
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("kinds")
    fun `receive out of time message`(kind: KindCase) {
        val controller = getController(kind)
        val kindToCall: Map<KindCase, IProcessor.() -> Unit> = mapOf(
            KindCase.MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            KindCase.RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
        )

        assertFailsWith<CrawlerHandleMessageException>("Check message before interval start") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(kind, KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START.minusNanos(1))
                }
            }.build())
        }
        verify(processor, never().description("Message before interval start"), kind, kindToCall)

        assertFailsWith<CrawlerHandleMessageException>("Check message with end interval timestamp") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(kind, KNOWN_BOOK, KNOWN_GROUP, INTERVAL_END)
                }
            }.build())
        }
        verify(processor, never().description("Message with end interval timestamp"), kind, kindToCall)

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
        ))
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("kinds")
    fun `receive correct messages`(kind: KindCase) {
        val minMessage = message(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START)
        val minRawMessage = rawMessage(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START)

        val controller = getController(kind)
        controller.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                when (kind) {
                    KindCase.MESSAGE -> this += minMessage
                    KindCase.RAW_MESSAGE -> this += minRawMessage
                    else -> error("Unsupported kind $kind")
                }
            }
        }.build())

        verify(processor, times(1).description("Message with start interval timestamp"), kind, mapOf(
            KindCase.MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(minMessage)) },
            KindCase.RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(minRawMessage)) },
        ))

        val intermediateMessage = message(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
        val intermediateRawMessage = rawMessage(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
        controller.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                when (kind) {
                    KindCase.MESSAGE -> this += intermediateMessage
                    KindCase.RAW_MESSAGE -> this += intermediateRawMessage
                    else -> error("Unsupported kind $kind")
                }
            }
        }.build())
        verify(processor, times(1).description("Message with half interval timestamp"), kind, mapOf(
            KindCase.MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(intermediateMessage)) },
            KindCase.RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(intermediateRawMessage)) },
        ))

        val maxMessage = message(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_END.minusNanos(1))
        val maxRawMessage = rawMessage(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_END.minusNanos(1))
        controller.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                when (kind) {
                    KindCase.MESSAGE -> this += maxMessage
                    KindCase.RAW_MESSAGE -> this += maxRawMessage
                    else -> error("Unsupported kind $kind")
                }
            }
        }.build())
        verify(processor, times(1).description("Message the nearest to the end interval timestamp"), kind, mapOf(
            KindCase.MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(maxMessage)) },
            KindCase.RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(maxRawMessage)) },
        ))

        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
        controller.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = KNOWN_GROUP }
                count = 3
            }
        }.build())

        verify(processor, never()).handle(eq(INTERVAL_EVENT_ID), any<Event>())
        verify(processor, never(), kind, mapOf(
            KindCase.MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            KindCase.RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
        ))

        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @Test
    fun `new controller`() {
        assertFalse(messageController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
        assertFalse(rawMessageController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @Test
    fun `receive several messages after pipeline`() {
        val builder = message(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START).toBuilder()
        val first = builder.apply {
            metadataBuilder.apply {
                idBuilder.apply {
                    sequence = 1
                    addAllSubsequence(listOf(1))
                }
            }
        }.build()
        val second = builder.apply {
            metadataBuilder.apply {
                idBuilder.apply {
                    sequence = 1
                    addAllSubsequence(listOf(2))
                }
            }
        }.build()

        messageController.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += first
                this += second
            }
        }.build())

        verify(processor, times(1).description("First message with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), eq(first))
        verify(processor, times(1).description("Second message with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), eq(second))
        verify(processor, never().description("Raw messages with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), any<RawMessage>())
        verify(processor, never().description("Events with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), any<Event>())

        assertFalse(messageController.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
        messageController.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = KNOWN_GROUP }
                count = 1
            }
        }.build())

        verify(processor, times(2).description("Final message handler verification")).handle(any(), any<Message>())
        verify(processor, never().description("Final raw message handler verification")).handle(any(), any<RawMessage>())
        verify(processor, never().description("Final event handler verification")).handle(any(), any<Event>())

        assertTrue(messageController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("kinds")
    fun `multiple expected method calls`(kind: KindCase) {
        val controller = getController(kind)
        val cycles = 2
        repeat(cycles) {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(kind, KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START)
                }
            }.build())
        }

        verify(processor, times(cycles).description("Handled messages"), kind, mapOf(
            KindCase.MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            KindCase.RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
        ))

        repeat(cycles) { cycle ->
            assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await not empty state, cycle: $cycle")
            controller.expected(MessageLoadedStatistic.newBuilder().apply {
                addStatBuilder().apply {
                    bookIdBuilder.apply { name = KNOWN_BOOK }
                    groupBuilder.apply { name = KNOWN_GROUP }
                    count = 1
                }
            }.build())
        }

        verify(processor, never()).handle(eq(INTERVAL_EVENT_ID), any<Event>())
        verify(processor, never(), kind, mapOf(
            KindCase.MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            KindCase.RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
        ))
        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    private fun getController(kind: KindCase) = when (kind) {
        KindCase.MESSAGE -> messageController
        KindCase.RAW_MESSAGE -> rawMessageController
        else -> error("Unsupported kind $kind")
    }

    private fun MessageGroup.Builder.message(kind: KindCase, book: String, group: String, timestamp: Instant) {
        when (kind) {
            KindCase.MESSAGE -> this += message(book, group, timestamp)
            KindCase.RAW_MESSAGE -> this += rawMessage(book, group, timestamp)
            else -> error("Unsupported kind $kind")
        }
    }
    private fun message(book: String, group: String, timestamp: Instant): Message = Message.newBuilder().apply {
        metadataBuilder.apply {
            idBuilder.apply {
                bookName = book
                this.timestamp = timestamp.toTimestamp()
                connectionIdBuilder.apply {
                    sessionGroup = group
                    sessionAlias = SESSION_ALIAS
                    sequence = SEQUENCE_COUNTER.incrementAndGet()
                }
            }
        }
    }.build()

    private fun rawMessage(book: String, group: String, timestamp: Instant): RawMessage = RawMessage.newBuilder().apply {
        metadataBuilder.apply {
            idBuilder.apply {
                bookName = book
                this.timestamp = timestamp.toTimestamp()
                connectionIdBuilder.apply {
                    sessionGroup = group
                    sessionAlias = SESSION_ALIAS
                    sequence = SEQUENCE_COUNTER.incrementAndGet()
                }
            }
        }
    }.build()

    companion object {
        private val SEQUENCE_COUNTER = AtomicLong(Random.nextLong())
        private val INTERVAL_HALF_LENGTH = Duration.ofMinutes(1)
        private val INTERVAL_LENGTH = INTERVAL_HALF_LENGTH + INTERVAL_HALF_LENGTH
        private val INTERVAL_START = Instant.now()
        private val INTERVAL_END = INTERVAL_START.plus(INTERVAL_LENGTH)
        private val INTERVAL_EVENT_ID = EventID.getDefaultInstance()

        private const val SESSION_ALIAS = "known-session-alias"
        private const val KNOWN_BOOK = "known-book"
        private const val UNKNOWN_BOOK = "unknown-book"
        private const val KNOWN_GROUP = "known-group"
        private const val UNKNOWN_GROUP = "unknown-group"
        private val BOOK_TO_GROUPS = mapOf(KNOWN_BOOK to setOf(KNOWN_GROUP))

        fun <T> verify(mock: T, mode: VerificationMode, kind: KindCase, calls: Map<KindCase, T.() -> Any>) {
            val call = requireNotNull(calls[kind])
            verify(mock, mode).call()
        }
        fun <T> verify(mock: T, mode: VerificationMode, calls: List<T.() -> Any>) {
            calls.forEach { call ->
                verify(mock, mode).call()
            }
        }
        @JvmStatic
        fun kinds() = listOf(
            Arguments.of(KindCase.MESSAGE),
            Arguments.of(KindCase.RAW_MESSAGE)
        )
    }
}