/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.processor.core.message

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.KNOWN_BOOK
import com.exactpro.th2.processor.KNOWN_GROUP
import com.exactpro.th2.processor.UNKNOWN_BOOK
import com.exactpro.th2.processor.UNKNOWN_GROUP
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.exactpro.th2.processor.core.configuration.MessageKind.MESSAGE
import com.exactpro.th2.processor.core.configuration.MessageKind.RAW_MESSAGE
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.verification.VerificationMode
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal abstract class AbstractCradleMessageGroupControllerTest<T> {

    protected val processor: IProcessor = mock { }

    abstract fun `put event expected value`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)
    abstract fun `put raw message actual value`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)
    abstract fun `put parsed message actual value`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)
    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `put empty expected value`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)
        assertDoesNotThrow("Pass empty expected") {
            controller.expected(MessageLoadedStatistic.getDefaultInstance())
        }
        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await completed state")
    }
    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `put unknown book expected value`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
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
    fun `put unknown group expected value (book and group only)`(
        bookToGroup: Map<String, Set<String>>,
        kinds: Set<MessageKind>
    ) {
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
    fun `put unknown group expected value (book only)`(
        bookToGroup: Map<String, Set<String>>,
        kinds: Set<MessageKind>
    ) {
        val controller = createController(bookToGroup, kinds)
        controller.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = UNKNOWN_GROUP }
            }
        }.build())
        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await completed state")
    }
    abstract fun `receive unknown book`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)
    abstract fun `receive unknown group (book and group only)`(
        bookToGroup: Map<String, Set<String>>,
        kinds: Set<MessageKind>
    )
    abstract fun `receive unknown group (book only)`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)
    abstract fun `receive out of time message`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)
    abstract fun `receive uncompleted decoding message group`(
        bookToGroup: Map<String, Set<String>>,
        kinds: Set<MessageKind>
    )
    abstract fun `receive correct messages`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)
    @ParameterizedTest
    @MethodSource("allCombinations")
    fun `new controller`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        assertFalse(createController(bookToGroup, kinds).await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }
    abstract fun `receive several messages after pipeline`(
        bookToGroup: Map<String, Set<String>>,
        kinds: Set<MessageKind>
    )
    abstract fun `multiple expected method calls`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)
    abstract fun `receive both kind of messages`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>)

    abstract fun createController(
        bookToGroup: Map<String, Set<String>>,
        kinds: Set<MessageKind>
    ): Controller<T>

    companion object {
        internal val INTERVAL_HALF_LENGTH = Duration.ofMinutes(1)
        private val INTERVAL_LENGTH = INTERVAL_HALF_LENGTH + INTERVAL_HALF_LENGTH
        internal val INTERVAL_START = Instant.now()
        internal val INTERVAL_END = INTERVAL_START.plus(INTERVAL_LENGTH)
        internal val INTERVAL_EVENT_ID = EventID.getDefaultInstance()
        internal val MESSAGE_KINDS = setOf(MESSAGE, RAW_MESSAGE)

        private val BOOK_TO_GROUPS = mapOf(KNOWN_BOOK to setOf(KNOWN_GROUP))
        private val BOOK_ONLY = mapOf(KNOWN_BOOK to emptySet<String>())

        fun <T> verify(mock: T, mode: VerificationMode, kinds: Set<MessageKind>, calls: Map<MessageKind, T.() -> Any>) {
            kinds.forEach { kind ->
                verify(mock, mode, kind, calls)
            }
        }

        private fun <T> verify(
            mock: T,
            mode: VerificationMode,
            kind: MessageKind,
            calls: Map<MessageKind, T.() -> Any>
        ) {
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