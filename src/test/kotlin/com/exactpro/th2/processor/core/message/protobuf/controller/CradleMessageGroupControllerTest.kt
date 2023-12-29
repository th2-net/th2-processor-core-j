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

package com.exactpro.th2.processor.core.message.protobuf.controller

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.KNOWN_BOOK
import com.exactpro.th2.processor.KNOWN_GROUP
import com.exactpro.th2.processor.SESSION_ALIAS
import com.exactpro.th2.processor.UNKNOWN_BOOK
import com.exactpro.th2.processor.UNKNOWN_GROUP
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.HandleMessageException
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.exactpro.th2.processor.core.configuration.MessageKind.MESSAGE
import com.exactpro.th2.processor.core.configuration.MessageKind.RAW_MESSAGE
import com.exactpro.th2.processor.core.message.AbstractCradleMessageGroupControllerTest
import com.exactpro.th2.processor.message
import com.exactpro.th2.processor.protobufMessage
import com.exactpro.th2.processor.protobufRawMessage
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.kotlin.any
import org.mockito.kotlin.eq
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class CradleMessageGroupControllerTest : AbstractCradleMessageGroupControllerTest<MessageGroupBatch>() {

    @ParameterizedTest
    @MethodSource("allCombinations")
    override fun `put event expected value`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        assertFailsWith<UnsupportedOperationException>("Call unsupported expected overload") {
            createController(bookToGroup, kinds).expected(EventLoadedStatistic.getDefaultInstance())
        }
    }

    @ParameterizedTest
    @MethodSource("parsedOnly")
    override fun `put raw message actual value`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        assertFailsWith<HandleMessageException>("Put incorrect message kind") {
            createController(bookToGroup, kinds).actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(
                        RAW_MESSAGE.grpcKind,
                        KNOWN_BOOK,
                        KNOWN_BOOK,
                        SESSION_ALIAS,
                        INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
                    )
                }
            }.build())
        }
    }

    @ParameterizedTest
    @MethodSource("rawOnly")
    override fun `put parsed message actual value`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        assertFailsWith<HandleMessageException>("Put incorrect message kind") {
            createController(bookToGroup, kinds).actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(
                        MESSAGE.grpcKind,
                        KNOWN_BOOK,
                        KNOWN_BOOK,
                        SESSION_ALIAS,
                        INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
                    )
                }
            }.build())
        }
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    override fun `receive unknown book`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)
        assertFailsWith<HandleMessageException>("Check message with unknown book") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(
                            kind.grpcKind,
                            UNKNOWN_BOOK,
                            KNOWN_GROUP,
                            SESSION_ALIAS,
                            INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
                        )
                    }
                }
            }.build())
        }

        verify(
            processor, never(), listOf(
                { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
                { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
                { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
            )
        )
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("bookAndGroupOnly")
    override fun `receive unknown group (book and group only)`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)
        assertFailsWith<HandleMessageException>("Check message with unknown group") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(
                            kind.grpcKind,
                            KNOWN_BOOK,
                            UNKNOWN_GROUP,
                            SESSION_ALIAS,
                            INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
                        )
                    }
                }
            }.build())
        }

        verify(
            processor, never(), listOf(
                { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
                { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
                { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
            )
        )
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("bookOnly")
    override fun `receive unknown group (book only)`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)
        controller.actual(MessageGroupBatch.newBuilder().apply {
            kinds.forEach { kind ->
                addGroupsBuilder().apply {
                    message(
                        kind.grpcKind,
                        KNOWN_BOOK,
                        UNKNOWN_GROUP,
                        SESSION_ALIAS,
                        INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
                    )
                }
            }
        }.build())

        verify(
            processor, times(1).description("Message with start interval timestamp"), kinds, mapOf(
                MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
                RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            )
        )
        verify(processor, never().description("Final event handler verification")).handle(any(), any<Event>())
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    override fun `receive out of time message`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)
        val kindToCall: Map<MessageKind, IProcessor.() -> Unit> = mapOf(
            MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
            RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
        )

        assertFailsWith<HandleMessageException>("Check message before interval start") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(
                            kind.grpcKind,
                            KNOWN_BOOK,
                            KNOWN_GROUP,
                            SESSION_ALIAS,
                            INTERVAL_START.minusNanos(1),
                        )
                    }
                }
            }.build())
        }
        verify(processor, never().description("Message before interval start"), kinds, kindToCall)

        assertFailsWith<HandleMessageException>("Check message with end interval timestamp") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(
                            kind.grpcKind,
                            KNOWN_BOOK,
                            KNOWN_GROUP,
                            SESSION_ALIAS,
                            INTERVAL_END,
                        )
                    }
                }
            }.build())
        }
        verify(processor, never().description("Message with end interval timestamp"), kinds, kindToCall)

        verify(
            processor, never(), listOf(
                { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
                { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
                { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
            )
        )
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allKindsOnly")
    override fun `receive uncompleted decoding message group`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)

        assertFailsWith<HandleMessageException>("Check uncompleted decoding message group") {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                addGroupsBuilder().apply {
                    message(
                        RAW_MESSAGE.grpcKind,
                        KNOWN_BOOK,
                        UNKNOWN_GROUP,
                        SESSION_ALIAS,
                        INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
                        1L
                    )
                    message(
                        MESSAGE.grpcKind,
                        KNOWN_BOOK,
                        UNKNOWN_GROUP,
                        SESSION_ALIAS,
                        INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
                        1L
                    )
                }
            }.build())
        }

        verify(
            processor, never().description("Final handler verification"), listOf(
                { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
                { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
                { handle(eq(INTERVAL_EVENT_ID), any<Event>()) },
            )
        )
        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    override fun `receive correct messages`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val minMessage =
            protobufMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, INTERVAL_START)
        val minRawMessage =
            protobufRawMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, INTERVAL_START)

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

        verify(
            processor, times(1).description("Message with start interval timestamp"), kinds, mapOf(
                MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(minMessage)) },
                RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(minRawMessage)) },
            )
        )

        val intermediateMessage = protobufMessage(
            KNOWN_BOOK,
            KNOWN_GROUP,
            SESSION_ALIAS,
            INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
        )
        val intermediateRawMessage = protobufRawMessage(
            KNOWN_BOOK,
            KNOWN_GROUP,
            SESSION_ALIAS,
            INTERVAL_START.plus(INTERVAL_HALF_LENGTH),
        )
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
        verify(
            processor, times(1).description("Message with half interval timestamp"), kinds, mapOf(
                MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(intermediateMessage)) },
                RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(intermediateRawMessage)) },
            )
        )

        val maxMessage = protobufMessage(
            KNOWN_BOOK,
            KNOWN_GROUP,
            SESSION_ALIAS,
            INTERVAL_END.minusNanos(1),
        )
        val maxRawMessage = protobufRawMessage(
            KNOWN_BOOK,
            KNOWN_GROUP,
            SESSION_ALIAS,
            INTERVAL_END.minusNanos(1),
        )
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
        verify(
            processor, times(1).description("Message the nearest to the end interval timestamp"), kinds, mapOf(
                MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(maxMessage)) },
                RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), eq(maxRawMessage)) },
            )
        )

        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
        controller.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = KNOWN_GROUP }
                count = 3
            }
        }.build())

        verify(processor, never()).handle(eq(INTERVAL_EVENT_ID), any<Event>())
        verify(
            processor, never(), MESSAGE_KINDS.minus(kinds), mapOf(
                MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
                RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            )
        )

        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("parsedOnly")
    override fun `receive several messages after pipeline`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)
        val builder = protobufMessage(
            KNOWN_BOOK,
            KNOWN_GROUP,
            SESSION_ALIAS,
            INTERVAL_START,
        ).toBuilder()
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

        verify(processor, times(1).description("First message with start interval timestamp")).handle(
            eq(
                INTERVAL_EVENT_ID
            ), eq(first)
        )
        verify(processor, times(1).description("Second message with start interval timestamp")).handle(
            eq(
                INTERVAL_EVENT_ID
            ), eq(second)
        )
        verify(
            processor,
            never().description("Raw messages with start interval timestamp")
        ).handle(eq(INTERVAL_EVENT_ID), any<RawMessage>())
        verify(processor, never().description("Events with start interval timestamp")).handle(
            eq(INTERVAL_EVENT_ID),
            any<Event>()
        )

        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await not empty state")
        controller.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = KNOWN_GROUP }
                count = 1
            }
        }.build())

        verify(processor, times(2).description("Final message handler verification")).handle(any(), any<Message>())
        verify(processor, never().description("Final raw message handler verification")).handle(
            any(),
            any<RawMessage>()
        )
        verify(processor, never().description("Final event handler verification")).handle(any(), any<Event>())

        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("allCombinations")
    override fun `multiple expected method calls`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)
        val cycles = 2
        repeat(cycles) {
            controller.actual(MessageGroupBatch.newBuilder().apply {
                kinds.forEach { kind ->
                    addGroupsBuilder().apply {
                        message(
                            kind.grpcKind,
                            KNOWN_BOOK,
                            KNOWN_GROUP,
                            SESSION_ALIAS,
                            INTERVAL_START,
                        )
                    }
                }
            }.build())
        }

        verify(
            processor, times(cycles).description("Handled messages"), kinds, mapOf(
                MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
                RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            )
        )

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
        verify(
            processor, never(), MESSAGE_KINDS.minus(kinds), mapOf(
                MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<Message>()) },
                RAW_MESSAGE to { handle(eq(INTERVAL_EVENT_ID), any<RawMessage>()) },
            )
        )
        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await empty state")
    }

    @ParameterizedTest
    @MethodSource("allKindsOnly")
    override fun `receive both kind of messages`(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>) {
        val controller = createController(bookToGroup, kinds)
        val raw = protobufRawMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, INTERVAL_START, 1)
        val builder = protobufMessage(
            KNOWN_BOOK,
            KNOWN_GROUP,
            SESSION_ALIAS,
            INTERVAL_START,
        ).toBuilder()
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

        verify(processor, times(1).description("First message with start interval timestamp")).handle(
            eq(
                INTERVAL_EVENT_ID
            ), eq(first)
        )
        verify(processor, times(1).description("Second message with start interval timestamp")).handle(
            eq(
                INTERVAL_EVENT_ID
            ), eq(second)
        )
        verify(
            processor,
            times(1).description("Raw message with start interval timestamp")
        ).handle(eq(INTERVAL_EVENT_ID), eq(raw))
        verify(processor, never().description("Events with start interval timestamp")).handle(
            eq(INTERVAL_EVENT_ID),
            any<Event>()
        )

        assertFalse(controller.await(1, TimeUnit.NANOSECONDS), "Await uncompleted state")
        controller.expected(MessageLoadedStatistic.newBuilder().apply {
            addStatBuilder().apply {
                bookIdBuilder.apply { name = KNOWN_BOOK }
                groupBuilder.apply { name = KNOWN_GROUP }
                count = 1
            }
        }.build())

        verify(processor, times(2).description("Final message handler verification")).handle(any(), any<Message>())
        verify(processor, times(1).description("Final raw message handler verification")).handle(
            any(),
            any<RawMessage>()
        )
        verify(processor, never().description("Final event handler verification")).handle(any(), any<Event>())

        assertTrue(controller.await(1, TimeUnit.NANOSECONDS), "Await completed state")
    }

    override fun createController(bookToGroup: Map<String, Set<String>>, kinds: Set<MessageKind>): Controller<MessageGroupBatch> =
        CradleMessageGroupController(
            processor,
            INTERVAL_EVENT_ID,
            INTERVAL_START.toTimestamp(),
            INTERVAL_END.toTimestamp(),
            kinds,
            bookToGroup
        )
}