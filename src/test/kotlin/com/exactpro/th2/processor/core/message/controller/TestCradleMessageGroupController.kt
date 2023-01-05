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

package com.exactpro.th2.processor.core.message.controller

import com.exactpro.th2.common.grpc.AnyMessage.KindCase
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
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

internal class TestCradleMessageGroupController {

    private val processor: IProcessor = mock {  }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `put event expected value`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        assertFailsWith<UnsupportedOperationException>("Call unsupported expected overload") {
            createController(bookToGroup, kinds).expected(EventLoadedStatistic.getDefaultInstance())
        }
    }
    @ParameterizedTest
    @MethodSource("parsedOnly")
    fun `put raw message actual value`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        assertFailsWith<CrawlerHandleMessageException>("Put incorrect message kind") {
            createController(bookToGroup, kinds).actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(RAW_MESSAGE, KNOWN_BOOK, KNOWN_BOOK, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
                }
            }.build())
        }
    }
    @ParameterizedTest
    @MethodSource("rawOnly")
    fun `put parsed message actual value`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        assertFailsWith<CrawlerHandleMessageException>("Put incorrect message kind") {
            createController(bookToGroup, kinds).actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(MESSAGE, KNOWN_BOOK, KNOWN_BOOK, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
                }
            }.build())
        }
    }
    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `put empty expected value`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
        assertDoesNotThrow("Pass empty expected") {
            controller.expected(MessageLoadedStatistic.getDefaultInstance())
        }
        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await completed state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `put unknown book expected value`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
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
    @MethodSource("bookAndGroupOnly")
    fun `put unknown group expected value (book and group only)`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
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
    @MethodSource("bookOnly")
    fun `put unknown group expected value (book only)`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
        controller.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = UNKNOWN_GROUP }
            }
        }.build())
        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await completed state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `receive unknown book`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
        assertFailsWith<CrawlerHandleMessageException>("Check message with unknown book") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(kind, UNKNOWN_BOOK, KNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
                    }
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
    @MethodSource("bookAndGroupOnly")
    fun `receive unknown group (book and group only)`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
        assertFailsWith<CrawlerHandleMessageException>("Check message with unknown group") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(kind, KNOWN_BOOK, UNKNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
                    }
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
    @MethodSource("bookOnly")
    fun `receive unknown group (book only)`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
        controller.actual(MessageGroupBatch.newBuilder().apply {
            kinds.forEach { kind ->
                addGroupsBuilder().apply {
                    message(kind, KNOWN_BOOK, UNKNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
                }
            }
        }.build())

        verify(processor, times(1).description("Message with start interval timestamp"), kinds, mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
        ))
        verify(processor, never().description("Final event handler verification")).handle(any(), any<Event>())
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `receive out of time message`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
        val kindToCall: Map<KindCase, IProcessor.() -> Unit> = mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
        )

        assertFailsWith<CrawlerHandleMessageException>("Check message before interval start") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(kind, KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START.minusNanos(1))
                    }
                }
            }.build())
        }
        verify(processor, never().description("Message before interval start"), kinds, kindToCall)

        assertFailsWith<CrawlerHandleMessageException>("Check message with end interval timestamp") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(kind, KNOWN_BOOK, KNOWN_GROUP, INTERVAL_END)
                    }
                }
            }.build())
        }
        verify(processor, never().description("Message with end interval timestamp"), kinds, kindToCall)

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
        ))
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allKindsOnly")
    fun `receive uncompleted decoding message group`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)

        assertFailsWith<CrawlerHandleMessageException>("Check uncompleted decoding message group") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(RAW_MESSAGE, KNOWN_BOOK, UNKNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH), 1L)
                    message(MESSAGE, KNOWN_BOOK, UNKNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH), 1L)
                }
            }.build())
        }

        verify(processor, never().description("Final handler verification"), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
        ))
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `receive correct messages`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val minMessage = message(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START)
        val minRawMessage = rawMessage(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START)

        val controller = createController(bookToGroup, kinds)
        controller.actual(MessageGroupBatch.newBuilder().apply {
            kinds.forEach { kind ->
                addGroupsBuilder().apply {
                    when (kind) {
                        MESSAGE -> this += minMessage
                        RAW_MESSAGE -> this += minRawMessage
                        else -> error("Unsupported kind $kind")
                    }
                }
            }
        }.build())

        verify(processor, times(1).description("Message with start interval timestamp"), kinds, mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(minMessage)) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(minRawMessage)) },
        ))

        val intermediateMessage = message(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
        val intermediateRawMessage = rawMessage(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
        controller.actual(MessageGroupBatch.newBuilder().apply {
            kinds.forEach { kind ->
                addGroupsBuilder().apply {
                    when (kind) {
                        MESSAGE -> this += intermediateMessage
                        RAW_MESSAGE -> this += intermediateRawMessage
                        else -> error("Unsupported kind $kind")
                    }
                }
            }
        }.build())
        verify(processor, times(1).description("Message with half interval timestamp"), kinds, mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(intermediateMessage)) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(intermediateRawMessage)) },
        ))

        val maxMessage = message(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_END.minusNanos(1))
        val maxRawMessage = rawMessage(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_END.minusNanos(1))
        controller.actual(MessageGroupBatch.newBuilder().apply {
            kinds.forEach { kind ->
                addGroupsBuilder().apply {
                    when (kind) {
                        MESSAGE -> this += maxMessage
                        RAW_MESSAGE -> this += maxRawMessage
                        else -> error("Unsupported kind $kind")
                    }
                }
            }
        }.build())
        verify(processor, times(1).description("Message the nearest to the end interval timestamp"), kinds, mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(maxMessage)) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(maxRawMessage)) },
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
        verify(processor, never(), MESSAGE_KINDS.minus(kinds), mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
        ))

        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `new controller`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        assertFalse(createController(bookToGroup, kinds).await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("parsedOnly")
    fun `receive several messages after pipeline`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
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

        controller.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += first
                this += second
            }
        }.build())

        verify(processor, times(1).description("First message with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), eq(first))
        verify(processor, times(1).description("Second message with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), eq(second))
        verify(processor, never().description("Raw messages with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), any<RawMessage>())
        verify(processor, never().description("Events with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), any<Event>())

        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
        controller.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = KNOWN_GROUP }
                count = 1
            }
        }.build())

        verify(processor, times(2).description("Final message handler verification")).handle(any(), any<Message>())
        verify(processor, never().description("Final raw message handler verification")).handle(any(), any<RawMessage>())
        verify(processor, never().description("Final event handler verification")).handle(any(), any<Event>())

        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `multiple expected method calls`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
        val cycles = 2
        repeat(cycles) {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(kind, KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START)
                    }
                }
            }.build())
        }

        verify(processor, times(cycles).description("Handled messages"), kinds, mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
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
        verify(processor, never(), MESSAGE_KINDS.minus(kinds), mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
        ))
        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("allKindsOnly")
    fun `receive both kind of messages`(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) {
        val controller = createController(bookToGroup, kinds)
        val raw = rawMessage(KNOWN_BOOK, KNOWN_GROUP, INTERVAL_START, 1)
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

        controller.actual(MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += first
                this += second
            }
            addGroupsBuilder().apply {
                this += raw
            }
        }.build())

        verify(processor, times(1).description("First message with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), eq(first))
        verify(processor, times(1).description("Second message with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), eq(second))
        verify(processor, times(1).description("Raw message with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), eq(raw))
        verify(processor, never().description("Events with start interval timestamp")).handle(eq(INTERVAL_EVENT_ID), any<Event>())

        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
        controller.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = KNOWN_GROUP }
                count = 1
            }
        }.build())

        verify(processor, times(2).description("Final message handler verification")).handle(any(), any<Message>())
        verify(processor, times(1).description("Final raw message handler verification")).handle(any(), any<RawMessage>())
        verify(processor, never().description("Final event handler verification")).handle(any(), any<Event>())

        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await completed state")
    }

    private fun createController(bookToGroup: Map<String, Set<String>>, kinds: Set<KindCase>) = CradleMessageGroupController(
        processor,
        INTERVAL_EVENT_ID,
        INTERVAL_START.toTimestamp(),
        INTERVAL_END.toTimestamp(),
        kinds,
        bookToGroup)

    private fun MessageGroup.Builder.message(
        kind: KindCase,
        book: String,
        group: String,
        timestamp: Instant,
        sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
    ) {
        when (kind) {
            MESSAGE -> this += message(book, group, timestamp, sequence)
            RAW_MESSAGE -> this += rawMessage(book, group, timestamp, sequence)
            else -> error("Unsupported kind $kind")
        }
    }
    private fun message(
        book: String,
        group: String,
        timestamp: Instant,
        sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
    ): Message = Message.newBuilder().apply {
        metadataBuilder.apply {
            idBuilder.apply {
                bookName = book
                this.timestamp = timestamp.toTimestamp()
                this.sequence = sequence
                connectionIdBuilder.apply {
                    sessionGroup = group
                    sessionAlias = SESSION_ALIAS
                }
            }
        }
    }.build()

    private fun rawMessage(book: String, group: String, timestamp: Instant, sequence: Long = SEQUENCE_COUNTER.incrementAndGet()): RawMessage = RawMessage.newBuilder().apply {
        metadataBuilder.apply {
            idBuilder.apply {
                bookName = book
                this.timestamp = timestamp.toTimestamp()
                this.sequence = sequence
                connectionIdBuilder.apply {
                    sessionGroup = group
                    sessionAlias = SESSION_ALIAS
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
        private val MESSAGE_KINDS = setOf(MESSAGE, RAW_MESSAGE)

        private const val SESSION_ALIAS = "known-session-alias"
        private const val KNOWN_BOOK = "known-book"
        private const val UNKNOWN_BOOK = "unknown-book"
        private const val KNOWN_GROUP = "known-group"
        private const val UNKNOWN_GROUP = "unknown-group"
        private val BOOK_TO_GROUPS = mapOf(KNOWN_BOOK to setOf(KNOWN_GROUP))
        private val BOOK_ONLY = mapOf(KNOWN_BOOK to emptySet<String>())

        fun <T> verify(mock: T, mode: VerificationMode, kinds: Set<KindCase>, calls: Map<KindCase, T.() -> Any>) {
            kinds.forEach { kind ->
                verify(mock, mode, kind, calls)
            }
        }
        private fun <T> verify(mock: T, mode: VerificationMode, kind: KindCase, calls: Map<KindCase, T.() -> Any>) {
            val call = requireNotNull(calls[kind])
            verify(mock, mode).call()
        }
        fun <T> verify(mock: T, mode: VerificationMode, calls: List<T.() -> Any>) {
            calls.forEach { call ->
                verify(mock, mode).call()
            }
        }
        @JvmStatic
        fun allKindsOnly() = listOf(
            Arguments.of(BOOK_ONLY, MESSAGE_KINDS),
            Arguments.of(BOOK_TO_GROUPS, MESSAGE_KINDS),
        )
        @JvmStatic
        fun allCombinations() = listOf(
            Arguments.of(BOOK_ONLY, setOf(MESSAGE)),
            Arguments.of(BOOK_ONLY, setOf(RAW_MESSAGE)),
            Arguments.of(BOOK_ONLY, MESSAGE_KINDS),
            Arguments.of(BOOK_TO_GROUPS, setOf(MESSAGE)),
            Arguments.of(BOOK_TO_GROUPS, setOf(RAW_MESSAGE)),
            Arguments.of(BOOK_TO_GROUPS, MESSAGE_KINDS),
        )

        @JvmStatic
        fun parsedOnly() = listOf(
            Arguments.of(BOOK_ONLY, setOf(MESSAGE)),
            Arguments.of(BOOK_TO_GROUPS, setOf(MESSAGE)),
        )

        @JvmStatic
        fun rawOnly() = listOf(
            Arguments.of(BOOK_ONLY, setOf(RAW_MESSAGE)),
            Arguments.of(BOOK_TO_GROUPS, setOf(RAW_MESSAGE)),
        )

        @JvmStatic
        fun bookAndGroupOnly() = listOf(
            Arguments.of(BOOK_TO_GROUPS, setOf(MESSAGE)),
            Arguments.of(BOOK_TO_GROUPS, setOf(RAW_MESSAGE)),
            Arguments.of(BOOK_TO_GROUPS, MESSAGE_KINDS),
        )

        @JvmStatic
        fun bookOnly() = listOf(
            Arguments.of(BOOK_ONLY, setOf(MESSAGE)),
            Arguments.of(BOOK_ONLY, setOf(RAW_MESSAGE)),
            Arguments.of(BOOK_ONLY, MESSAGE_KINDS),
        )
    }
}