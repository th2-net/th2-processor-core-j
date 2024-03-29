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

package com.exactpro.th2.processor.core.event.controller

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.KNOWN_BOOK
import com.exactpro.th2.processor.KNOWN_SCOPE
import com.exactpro.th2.processor.UNKNOWN_BOOK
import com.exactpro.th2.processor.UNKNOWN_SCOPE
import com.exactpro.th2.processor.api.IProcessor
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
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class TestEventController {

    private var processor: IProcessor = mock {  }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `put message expected value`(bookToScope: Map<String, Set<String>>) {
        val eventController = createController(bookToScope)
        assertFailsWith<UnsupportedOperationException>("Call unsupported expected overload") {
            eventController.expected(MessageLoadedStatistic.getDefaultInstance())
        }
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `put empty expected value`(bookToScope: Map<String, Set<String>>) {
        val eventController = createController(bookToScope)
        assertDoesNotThrow("Pass empty expected") {
            eventController.expected(EventLoadedStatistic.getDefaultInstance())
        }
        assertTrue(eventController.await(1, TimeUnit.NANOSECONDS), "Await completed state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `put unknown book expected value`(bookToScope: Map<String, Set<String>>) {
        val eventController = createController(bookToScope)
        assertFailsWith<IllegalStateException>("Check statistic unknown book") {
            eventController.expected(EventLoadedStatistic.newBuilder().apply {
                addStatBuilder().apply {
                    bookIdBuilder.apply { name = UNKNOWN_BOOK }
                    scopeBuilder.apply { name = KNOWN_SCOPE }
                }
            }.build())
        }
        assertFalse(eventController.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @Test
    fun `put unknown scope expected value`() {
        val eventController = createController(BOOK_TO_SCOPES)
        assertFailsWith<IllegalStateException>("Check statistic unknown scope") {
            eventController.expected(EventLoadedStatistic.newBuilder().apply {
                addStatBuilder().apply {
                    bookIdBuilder.apply { name = KNOWN_BOOK }
                    scopeBuilder.apply { name = UNKNOWN_SCOPE }
                }
            }.build())
        }
        assertFalse(eventController.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `receive unknown book`(bookToScope: Map<String, Set<String>>) {
        val eventController = createController(bookToScope)
        assertFailsWith<IllegalStateException>("Check event with unknown book") {
            eventController.actual(EventBatch.newBuilder().apply {
                addEvents(event(UNKNOWN_BOOK, KNOWN_SCOPE, INTERVAL_START.plus(INTERVAL_HALF_LENGTH)))
            }.build())
        }

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
        ))
        assertFalse(eventController.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @Test
    fun `receive unknown scope`() {
        val eventController = createController(BOOK_TO_SCOPES)
        assertFailsWith<IllegalStateException>("Check event with unknown scope") {
            eventController.actual(EventBatch.newBuilder().apply {
                addEvents(event(KNOWN_BOOK, UNKNOWN_SCOPE, INTERVAL_START.plus(INTERVAL_HALF_LENGTH)))
            }.build())
        }

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
        ))
        assertFalse(eventController.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `receive out of time event`(bookToScope: Map<String, Set<String>>) {
        val eventController = createController(bookToScope)
        assertFailsWith<IllegalStateException>("Check event before interval start") {
            eventController.actual(EventBatch.newBuilder().apply {
                addEvents(event(KNOWN_BOOK, KNOWN_SCOPE, INTERVAL_START.minusNanos(1)))
            }.build())
        }
        verify(processor, never().description("Event before interval start")).handle(eq(INTERVAL_EVENT_ID), any<Event>())

        assertFailsWith<IllegalStateException>("Check event with end interval timestamp") {
            eventController.actual(EventBatch.newBuilder().apply {
                addEvents(event(KNOWN_BOOK, KNOWN_SCOPE, INTERVAL_END))
            }.build())
        }
        verify(processor, never().description("Event with end interval timestamp")).handle(eq(INTERVAL_EVENT_ID), any<Event>())

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
        ))
        assertFalse(eventController.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `new controller`(bookToScope: Map<String, Set<String>>) {
        val eventController = createController(bookToScope)
        assertFalse(eventController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `receive correct events`(bookToScope: Map<String, Set<String>>) {
        val eventController = createController(bookToScope)
        val minEvent = event(KNOWN_BOOK, KNOWN_SCOPE, INTERVAL_START)

        eventController.actual(EventBatch.newBuilder().apply {
            addEvents(minEvent)
        }.build())

        verify(processor, times(1).description("Event with start interval timestamp")).handle(
            eq(INTERVAL_EVENT_ID),
            eq(minEvent)
        )

        val intermediateEvent = event(KNOWN_BOOK, KNOWN_SCOPE, INTERVAL_START.plus(INTERVAL_HALF_LENGTH))
        eventController.actual(EventBatch.newBuilder().apply {
            addEvents(intermediateEvent)
        }.build())
        verify(processor, times(1).description("Event with half interval timestamp")).handle(
            eq(INTERVAL_EVENT_ID),
            eq(intermediateEvent)
        )

        val maxEvent = event(KNOWN_BOOK, KNOWN_SCOPE, INTERVAL_END.minusNanos(1))
        eventController.actual(EventBatch.newBuilder().apply {
            addEvents(maxEvent)
        }.build())
        verify(processor, times(1).description("Event the nearest to the end interval timestamp")).handle(
            eq(INTERVAL_EVENT_ID),
            eq(maxEvent)
        )

        assertFalse(eventController.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
        eventController.expected(EventLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                scopeBuilder.apply { name = KNOWN_SCOPE }
                count = 3
            }
        }.build())

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
        ))

        assertTrue(eventController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `multiple expected method calls`(bookToScope: Map<String, Set<String>>) {
        val eventController = createController(bookToScope)
        val cycles = 2
        repeat(cycles) {
            eventController.actual(EventBatch.newBuilder().apply {
                addEvents(event(KNOWN_BOOK, KNOWN_SCOPE, INTERVAL_START))
            }.build())
        }

        verify(processor, times(cycles).description("Handled events")).handle(eq(INTERVAL_EVENT_ID), any<Event>())

        repeat(cycles) {
            assertFalse(eventController.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
            eventController.expected(EventLoadedStatistic.newBuilder().apply {
                addStatBuilder().apply {
                    bookIdBuilder.apply { name = KNOWN_BOOK }
                    scopeBuilder.apply { name = KNOWN_SCOPE }
                    count = 1
                }
            }.build())
        }

        verify(processor, never(), listOf(
            { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
        ))
        assertTrue(eventController.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    private fun event(book: String, scope: String, timestamp: Instant): Event = Event.newBuilder().apply {
        idBuilder.apply {
            this.bookName = book
            this.scope = scope
            this.startTimestamp = timestamp.toTimestamp()
        }
    }.build()

    private fun createController(bookToScope: Map<String, Set<String>>) = EventController(
        processor,
        INTERVAL_EVENT_ID,
        INTERVAL_START.toTimestamp(),
        INTERVAL_END.toTimestamp(),
        bookToScope
    )

    companion object {
        private val INTERVAL_HALF_LENGTH = Duration.ofMinutes(1)
        private val INTERVAL_LENGTH = INTERVAL_HALF_LENGTH + INTERVAL_HALF_LENGTH
        private val INTERVAL_START = Instant.now()
        private val INTERVAL_END = INTERVAL_START.plus(INTERVAL_LENGTH)
        private val INTERVAL_EVENT_ID = EventID.getDefaultInstance()

        private val BOOK_TO_SCOPES = mapOf(KNOWN_BOOK to setOf(KNOWN_SCOPE))
        private val BOOK_ONLY = mapOf(KNOWN_BOOK to emptySet<String>())

        fun <T> verify(mock: T, mode: VerificationMode, calls: List<T.() -> Any>) {
            calls.forEach { call ->
                verify(mock, mode).call()
            }
        }

        @JvmStatic
        fun allCombinations() = listOf(
            Arguments.of(BOOK_ONLY),
            Arguments.of(BOOK_TO_SCOPES),
        )
    }
}