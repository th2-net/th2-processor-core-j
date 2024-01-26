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

package com.exactpro.th2.processor.core.state

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.event.transport.toProto
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.utils.message.transport.toProto
import com.exactpro.th2.common.utils.toInstant
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.processor.core.state.AbstractDataProviderStateStorage.Companion.METADATA_SIZE
import com.exactpro.th2.processor.core.state.AbstractDataProviderStateStorage.Companion.MIN_STATE_SIZE
import com.exactpro.th2.processor.core.state.StateType.Companion.METADATA_STATE_TYPE_PROPERTY
import com.exactpro.th2.processor.core.state.StateType.END
import com.exactpro.th2.processor.core.state.StateType.INTERMEDIATE
import com.exactpro.th2.processor.core.state.StateType.SINGLE
import com.exactpro.th2.processor.core.state.StateType.START
import com.google.protobuf.Timestamp
import com.google.protobuf.UnsafeByteOperations
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.mockito.ArgumentMatchers
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Instant
import java.util.function.Function
import java.util.function.Supplier
import kotlin.math.min
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import com.exactpro.th2.common.grpc.RawMessage as ProtobufRawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage as TransportRawMessage
import com.exactpro.th2.processor.core.state.protobuf.DataProviderStateStorage as ProtobufDataProviderStateStorage
import com.exactpro.th2.processor.core.state.transport.DataProviderStateStorage as TransportDataProviderStateStorage
import strikt.api.expectThat
import strikt.assertions.all
import strikt.assertions.contains
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.withElementAt

/**
 * The class included test for protobuf and transport modes
 */
internal class TestDataProviderStateStorage {

    private val eventBatcher = mock<EventBatcher> {  }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `max message size argument`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>
    ) {
        assertDoesNotThrow {
            stateStorageProvider.invoke(
                messageRouterSupplier.get(),
                eventBatcher,
                DUMMY_DATA_PROVIDER,
                BOOK_NAME,
                STATE_SESSION_ALIAS,
                MIN_STATE_SIZE + METADATA_SIZE
            )
        }

        assertFailsWith<IllegalStateException> {
            stateStorageProvider.invoke(
                messageRouterSupplier.get(),
                eventBatcher,
                DUMMY_DATA_PROVIDER,
                BOOK_NAME,
                STATE_SESSION_ALIAS,
                MIN_STATE_SIZE + METADATA_SIZE - 1
            )
        }
    }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `load state from unknown alias`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>
    ) {
        val responseIterator = sequence {
            while (true) {
                yield(emptyList<MessageSearchResponse>().iterator())
            }

        }.iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { responseIterator.next() }
        }

        val storage = stateStorageProvider.invoke(
            messageRouterSupplier.get(),
            eventBatcher,
            dataProvider,
            BOOK_NAME,
            STATE_SESSION_ALIAS,
            METADATA_SIZE + MIN_STATE_SIZE
        )
        assertNull(storage.loadState(EVENT_ID), "Load empty state")
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `load state from single message`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>
    ) {
        val responseIterator = sequence {
            yield(listOf(createMessageSearchResponse()).iterator())
            while (true) {
                yield(emptyList<MessageSearchResponse>().iterator())
            }
        }.iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { responseIterator.next() }
        }

        val storage = stateStorageProvider.invoke(
            messageRouterSupplier.get(),
            eventBatcher,
            dataProvider,
            BOOK_NAME,
            STATE_SESSION_ALIAS,
            METADATA_SIZE + MIN_STATE_SIZE
        )

        assertArrayEquals(STATE, storage.loadState(EVENT_ID), "Load state")
        verify(dataProvider, times(1).description("Number of search messages calls")).searchMessages(any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `load state from multiple message`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>
    ) {
        val parts = 4
        check(parts > 1) {
            "Number of parts is less than 1"
        }
        check(parts <= STATE.size) {
            "Number of parts is grater than ${STATE.size}"
        }

        val list = buildList {
            val partSize = (STATE.size + 1) / parts
            val timestamp = Instant.now().toTimestamp()
            repeat(parts) { index ->
                val revertedIndex = parts - 1 - index
                val data = STATE.copyOfRange(partSize * revertedIndex, min(STATE.size, partSize * (revertedIndex + 1)))
                val stateType = when (revertedIndex) {
                    0 -> START
                    parts - 1 -> END
                    else -> INTERMEDIATE
                }
                add(createMessageSearchResponse(stateType, timestamp, revertedIndex.toLong() + 1, data))
            }
        }

        val iterator = list.asSequence()
            .map { listOf(it).iterator() }
            .iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { iterator.next() }
        }

        val storage = stateStorageProvider.invoke(
            messageRouterSupplier.get(),
            eventBatcher,
            dataProvider,
            BOOK_NAME,
            STATE_SESSION_ALIAS,
            METADATA_SIZE + MIN_STATE_SIZE
        )
        assertArrayEquals(STATE, storage.loadState(EVENT_ID), "Load state")
        verify(dataProvider, times(parts).description("Number of search messages calls")).searchMessages(any())
        val captor = argumentCaptor<Event> { }
        verify(eventBatcher, times(1).description("Publish events")).onEvent(captor.capture())
        expectThat(captor.allValues.single()) {
            get { id }.apply {
                get { bookName }.isEqualTo(EVENT_ID.bookName)
                get { scope }.isEqualTo(EVENT_ID.scope)
            }
            get { parentId }.isEqualTo(EVENT_ID)
            get { name }.isEqualTo("Loaded state from $parts messages")
            get { type }.isEqualTo("Load state")
            get { status }.isEqualTo(EventStatus.SUCCESS)
            get { attachedMessageIdsList }.hasSize(parts)
                .contains(list.map { it.message.messageId })
            get { body.toStringUtf8() }.contains(
                Regex(
                    """
                            |[
                              |{"type":"State","rows":[
                                |{"type":"END","messageId":""$BOOK_NAME:$STATE_SESSION_ALIAS:first:\d+:${list[0].message.messageId.sequence}""},
                                |{"type":"INTERMEDIATE",""$BOOK_NAME:$STATE_SESSION_ALIAS:first:\d+:${list[1].message.messageId.sequence}""},
                                |{"type":"INTERMEDIATE",""$BOOK_NAME:$STATE_SESSION_ALIAS:first:\d+:${list[2].message.messageId.sequence}""},
                                |{"type":"START","messageId":""$BOOK_NAME:$STATE_SESSION_ALIAS:first:\d+:${list[3].message.messageId.sequence}""}
                              |]}
                            |]
                        """.trimMargin().replace("\n", "")
                )
            )
        }
    }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `load state after skip broken state by timestamp`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>
    ) {
        val now = Instant.now()
        val earlierTimestamp = now.minusNanos(1).toTimestamp()
        val nowTimestamp = now.toTimestamp()
        val laterTimestamp = now.plusNanos(1).toTimestamp()

        val list = listOf(
            createMessageSearchResponse(END, laterTimestamp, 8, STATE),
            createMessageSearchResponse(START, nowTimestamp, 7, STATE),

            createMessageSearchResponse(END, laterTimestamp, 6, STATE),
            createMessageSearchResponse(INTERMEDIATE, nowTimestamp, 5, STATE),
            createMessageSearchResponse(START, nowTimestamp, 4, STATE),

            createMessageSearchResponse(END, nowTimestamp, 3, STATE),
            createMessageSearchResponse(START, earlierTimestamp, 2, STATE),

            createMessageSearchResponse(SINGLE, nowTimestamp, 1, STATE),
        )
        val iterator = list.asSequence()
            .map { listOf(it).iterator() }
            .iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { iterator.next() }
        }

        val storage = stateStorageProvider.invoke(
            messageRouterSupplier.get(),
            eventBatcher,
            dataProvider,
            BOOK_NAME,
            STATE_SESSION_ALIAS,
            METADATA_SIZE + MIN_STATE_SIZE
        )
        assertArrayEquals(STATE, storage.loadState(EVENT_ID), "Load state")
        verify(dataProvider, times(8).description("Number of search messages calls")).searchMessages(any())
        val captor = argumentCaptor<Event> { }
        verify(eventBatcher, times(5).description("Publish events")).onEvent(captor.capture())
        expectThat(captor.allValues) {
            all {
                get { id }.apply {
                    get { bookName }.isEqualTo(EVENT_ID.bookName)
                    get { scope }.isEqualTo(EVENT_ID.scope)
                }
                get { parentId }.isEqualTo(EVENT_ID)
                get { type }.isEqualTo("Load state")
            }

            withElementAt(0) {
                get { name }.isEqualTo(
                    "Dropped state by broken timestamp, expected: ${list[0].message.messageId.timestamp.toInstant()}, " +
                            "actual: ${list[1].message.messageId.timestamp.toInstant()}"
                )
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.hasSize(2)
                    .contains(list[0].message.messageId,list[1].message.messageId)
                get { body.toStringUtf8() }.isEqualTo("[]")
            }
            withElementAt(1) {
                get { name }.isEqualTo(
                    "Dropped state by broken timestamp, expected: ${list[2].message.messageId.timestamp.toInstant()}, " +
                            "actual: ${list[3].message.messageId.timestamp.toInstant()}"
                )
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.hasSize(2)
                    .contains(list[2].message.messageId, list[3].message.messageId)
                get { body.toStringUtf8() }.isEqualTo("[]")
            }
            withElementAt(2) {
                get { name }.isEqualTo("Reversed state can't be start from START status type")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.hasSize(1)
                    .contains(list[4].message.messageId)
                get { body.toStringUtf8() }.isEqualTo("[]")
            }
            withElementAt(3) {
                get { name }.isEqualTo(
                    "Dropped state by broken timestamp, expected: ${list[5].message.messageId.timestamp.toInstant()}, " +
                            "actual: ${list[6].message.messageId.timestamp.toInstant()}"
                )
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.hasSize(2)
                    .contains(list[5].message.messageId,list[6].message.messageId)
                get { body.toStringUtf8() }.isEqualTo("[]")
            }
            withElementAt(4) {
                get { name }.isEqualTo("Loaded state from 1 messages")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { attachedMessageIdsList }.hasSize(1)
                    .contains(list[7].message.messageId)
                get { body.toStringUtf8() }.contains(Regex("""
                \[\{"type":"State","rows":\[\{"type":"SINGLE","messageId":"$BOOK_NAME:$STATE_SESSION_ALIAS:first:\d+:${list[7].message.messageId.sequence}"}]}]
                """.trimIndent()))
            }
        }
    }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `load state after skip broken state by sequence`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>
    ) {
        val timestamp = Instant.now().toTimestamp()
        val list = listOf(
            createMessageSearchResponse(END, timestamp, 12, STATE), // Gap
            createMessageSearchResponse(INTERMEDIATE, timestamp, 10, STATE),
            createMessageSearchResponse(START, timestamp, 8, STATE),

            createMessageSearchResponse(END, timestamp, 8, STATE),
            createMessageSearchResponse(INTERMEDIATE, timestamp, 7, STATE), // Gap
            createMessageSearchResponse(START, timestamp, 5, STATE),

            createMessageSearchResponse(END, timestamp, 4, STATE), // Gap
            createMessageSearchResponse(START, timestamp, 2, STATE),

            createMessageSearchResponse(SINGLE, timestamp, 1, STATE),
        )
        val iterator = list.asSequence()
            .map { listOf(it).iterator() }
            .iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { iterator.next() }
        }

        val storage = stateStorageProvider.invoke(
            messageRouterSupplier.get(),
            eventBatcher,
            dataProvider,
            BOOK_NAME,
            STATE_SESSION_ALIAS,
            METADATA_SIZE + MIN_STATE_SIZE
        )
        assertArrayEquals(STATE, storage.loadState(EVENT_ID), "Load state")
        verify(dataProvider, times(9).description("Number of search messages calls")).searchMessages(any())
        val captor = argumentCaptor<Event> { }
        verify(eventBatcher, times(5).description("Publish events")).onEvent(captor.capture())
        expectThat(captor.allValues) {
            all {
                get { id }.apply {
                    get { bookName }.isEqualTo(EVENT_ID.bookName)
                    get { scope }.isEqualTo(EVENT_ID.scope)
                }
                get { parentId }.isEqualTo(EVENT_ID)
                get { type }.isEqualTo("Load state")
            }

            withElementAt(0) {
                get { name }.isEqualTo(
                    "Dropped state by broken sequence, expected: ${list[0].message.messageId.sequence}, " +
                            "actual: ${list[1].message.messageId.sequence}"
                )
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.hasSize(2)
                    .contains(list[0].message.messageId,list[1].message.messageId)
                get { body.toStringUtf8() }.isEqualTo("[]")
            }
            withElementAt(1) {
                get { name }.isEqualTo("Reversed state can't be start from START status type")
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.hasSize(1)
                    .contains(list[2].message.messageId)
                get { body.toStringUtf8() }.isEqualTo("[]")
            }
            withElementAt(2) {
                get { name }.isEqualTo(
                    "Dropped state by broken sequence, expected: ${list[4].message.messageId.sequence}, " +
                            "actual: ${list[5].message.messageId.sequence}"
                )
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.hasSize(3)
                    .contains(list[3].message.messageId,list[4].message.messageId,list[5].message.messageId)
                get { body.toStringUtf8() }.isEqualTo("[]")
            }
            withElementAt(3) {
                get { name }.isEqualTo(
                    "Dropped state by broken sequence, expected: ${list[6].message.messageId.sequence}, " +
                            "actual: ${list[7].message.messageId.sequence}"
                )
                get { status }.isEqualTo(EventStatus.FAILED)
                get { attachedMessageIdsList }.hasSize(2)
                    .contains(list[6].message.messageId,list[7].message.messageId)
                get { body.toStringUtf8() }.isEqualTo("[]")
            }
            withElementAt(4) {
                get { name }.isEqualTo("Loaded state from 1 messages")
                get { status }.isEqualTo(EventStatus.SUCCESS)
                get { attachedMessageIdsList }.hasSize(1)
                    .contains(list[8].message.messageId)
                get { body.toStringUtf8() }.contains(Regex("""
                \[\{"type":"State","rows":\[\{"type":"SINGLE","messageId":"$BOOK_NAME:$STATE_SESSION_ALIAS:first:\d+:${list[8].message.messageId.sequence}"}]}]
                """.trimIndent()))
            }
        }
    }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `save state to single message`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>
    ) {
        val messageRouter: MessageRouter<T> = messageRouterSupplier.get()
        val data = ByteArray(MIN_STATE_SIZE)

        val storage = stateStorageProvider.invoke(
            messageRouter,
            eventBatcher,
            DUMMY_DATA_PROVIDER,
            BOOK_NAME,
            STATE_SESSION_ALIAS,
            METADATA_SIZE + MIN_STATE_SIZE
        )
        storage.saveState(EVENT_ID, data)
        verify(messageRouter, times(1).description("State parts")).sendAll(ArgumentMatchers.any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `save state to multiple messages`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>
    ) {
        val messageRouter: MessageRouter<T> = messageRouterSupplier.get()
        val parts = 3
        val data = ByteArray(parts * MIN_STATE_SIZE).apply(Random::nextBytes)

        val storage = stateStorageProvider.invoke(
            messageRouter,
            eventBatcher,
            DUMMY_DATA_PROVIDER,
            BOOK_NAME,
            STATE_SESSION_ALIAS,
            METADATA_SIZE + MIN_STATE_SIZE
        )
        storage.saveState(EVENT_ID, data)
        verify(messageRouter, times(parts).description("State parts")).sendAll(ArgumentMatchers.any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @ParameterizedTest
    @MethodSource("stateStorages")
    fun <T> `save and load state`(
        stateStorageProvider: StateStorageProvider<T>,
        messageRouterSupplier: Supplier<MessageRouter<T>>,
        batchConverter: Function<T, MessageGroupBatch>
    ) {
        val cache = mutableListOf<MessageGroupBatch>()
        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer {
                cache.reversed()
                    .map { batch ->
                        MessageSearchResponse.newBuilder().apply {
                            messageBuilder.apply {
                                val rawMessage = batch.getGroups(0).getMessages(0).rawMessage
                                messageId = rawMessage.id
                                putAllMessageProperties(rawMessage.metadata.propertiesMap)
                                bodyRaw = rawMessage.body
                            }
                        }.build()
                    }.iterator()
            }
        }
        val messageRouter: MessageRouter<T> = messageRouterSupplier.get()
        whenever(messageRouter.sendAll(ArgumentMatchers.any())).doAnswer {
            @Suppress("UNCHECKED_CAST")
            cache.add(batchConverter.apply(it.arguments[0] as T))
            return@doAnswer Unit
        }
        val storage = stateStorageProvider.invoke(
            messageRouter,
            eventBatcher,
            dataProvider,
            BOOK_NAME,
            STATE_SESSION_ALIAS,
            METADATA_SIZE + MIN_STATE_SIZE
        )

        val singleData = ByteArray(MIN_STATE_SIZE).apply(Random::nextBytes)
        storage.saveState(EVENT_ID, singleData)
        verify(messageRouter, times(1).description("Single state is published as single raw message"))
            .sendAll(ArgumentMatchers.argThat { batch -> batchConverter.apply(batch).verifyBatch(SINGLE, singleData) })
        assertEquals(1, cache.size, "Single state in cache")
        assertArrayEquals(singleData, storage.loadState(EVENT_ID), "Loaded single data")
        verify(dataProvider, times(1).description("Number of search messages calls")).searchMessages(any())

        val doubleData = ByteArray(MIN_STATE_SIZE * 2).apply(Random::nextBytes)
        storage.saveState(EVENT_ID, doubleData)
        verify(messageRouter, times(1).description("First part of double state is published as start raw message"))
            .sendAll(ArgumentMatchers.argThat { batch -> batchConverter.apply(batch).verifyBatch(START, doubleData.copyOfRange(0, MIN_STATE_SIZE)) })
        verify(messageRouter, times(1).description("Second part of double state is published as start raw message"))
            .sendAll(ArgumentMatchers.argThat { batch -> batchConverter.apply(batch).verifyBatch(END, doubleData.copyOfRange(MIN_STATE_SIZE, MIN_STATE_SIZE * 2)) })
        assertEquals(3, cache.size, "Double state in cache")
        assertArrayEquals(doubleData, storage.loadState(EVENT_ID), "Loaded double data")
        verify(dataProvider, times(2).description("Number of search messages calls")).searchMessages(any())

        val tripleData = ByteArray(MIN_STATE_SIZE * 3).apply(Random::nextBytes)
        storage.saveState(EVENT_ID, tripleData)
        verify(messageRouter, times(1).description("First part of triple state is published as start raw message"))
            .sendAll(ArgumentMatchers.argThat { batch -> batchConverter.apply(batch).verifyBatch(START, tripleData.copyOfRange(0, MIN_STATE_SIZE)) })
        verify(messageRouter, times(1).description("Second part of triple state is published as start raw message"))
            .sendAll(ArgumentMatchers.argThat { batch -> batchConverter.apply(batch).verifyBatch(INTERMEDIATE, tripleData.copyOfRange(MIN_STATE_SIZE, MIN_STATE_SIZE * 2)) })
        verify(messageRouter, times(1).description("Third part of triple state is published as start raw message"))
            .sendAll(ArgumentMatchers.argThat { batch -> batchConverter.apply(batch).verifyBatch(END, tripleData.copyOfRange(MIN_STATE_SIZE * 2, MIN_STATE_SIZE * 3)) })
        assertEquals(6, cache.size, "Triple state in cache")
        assertArrayEquals(tripleData, storage.loadState(EVENT_ID), "Loaded triple data")
        verify(dataProvider, times(3).description("Number of search messages calls")).searchMessages(any())
    }

    private fun MessageGroupBatch.verifyBatch(stateType: StateType, data: ByteArray): Boolean {
        val groups = groupsList
        if (groups.size != 1) { return false }
        val messages = groups[0].messagesList
        if (messages.size != 1) { return false }
        val anyMessage = messages[0]
        if (!anyMessage.hasRawMessage()) { return false }
        val rawMessage = anyMessage.rawMessage
        return rawMessage.metadata.propertiesMap[METADATA_STATE_TYPE_PROPERTY] == stateType.name
                && rawMessage.body.toByteArray().contentEquals(data)
    }

    companion object {
        private const val BOOK_NAME = "book"
        private const val STATE_SESSION_ALIAS = "state"

        private val EVENT_ID = EventID.newBuilder().apply {
            bookName = "book"
            scope = "scope"
            id = "id"
            startTimestamp = Instant.now().toTimestamp()
        }.build()
        private val STATE: ByteArray = "Hello world".toByteArray()

        private val DUMMY_DATA_PROVIDER: DataProviderService = mock { }

        private val PROTOBUF_BATCH_CONVERTER = object : Function<MessageGroupBatch, MessageGroupBatch> {
            override fun apply(t: MessageGroupBatch): MessageGroupBatch = t
            override fun toString(): String = "protobuf"
        }

        private val TRANSPORT_BATCH_CONVERTER = object : Function<GroupBatch, MessageGroupBatch> {
            override fun apply(t: GroupBatch): MessageGroupBatch = MessageGroupBatch.newBuilder().apply {
                t.groups.forEach { g ->
                    addGroupsBuilder().apply {
                        g.messages.forEach { m ->
                            addMessagesBuilder().apply {
                                when (m) {
                                    is ParsedMessage -> message = m.toProto(t.book, t.sessionGroup)
                                    is TransportRawMessage -> rawMessage = m.toProto(t.book, t.sessionGroup)
                                }
                            }
                        }
                    }
                }
            }.build()
            override fun toString(): String = "transport"
        }

        private val PROTOBUF_MESSAGE_STORAGE_SUPPLIER = object : Supplier<MessageRouter<MessageGroupBatch>> {
            override fun get(): MessageRouter<MessageGroupBatch> = mock { }
            override fun toString(): String = "protobuf"
        }

        private val TRANSPORT_MESSAGE_STORAGE_SUPPLIER = object : Supplier<MessageRouter<GroupBatch>> {
            override fun get(): MessageRouter<GroupBatch> = mock { }
            override fun toString(): String = "transport"
        }

        private val PROTOBUF_STATE_STORAGE_PROVIDER = object : StateStorageProvider<MessageGroupBatch> {
            override fun invoke(
                messageRouter: MessageRouter<MessageGroupBatch>,
                eventBatcher: EventBatcher,
                dataProvider: DataProviderService,
                bookName: String,
                stateSessionAlias: String,
                maxMessageSize: Long
            ): AbstractDataProviderStateStorage<MessageGroupBatch> = ProtobufDataProviderStateStorage(
                messageRouter,
                eventBatcher,
                dataProvider,
                bookName,
                stateSessionAlias,
                maxMessageSize
            )

            override fun toString(): String = "protobuf"
        }

        private val TRANSPORT_STATE_STORAGE_PROVIDER = object : StateStorageProvider<GroupBatch> {
            override fun invoke(
                messageRouter: MessageRouter<GroupBatch>,
                eventBatcher: EventBatcher,
                dataProvider: DataProviderService,
                bookName: String,
                stateSessionAlias: String,
                maxMessageSize: Long
            ): AbstractDataProviderStateStorage<GroupBatch> = TransportDataProviderStateStorage(
                messageRouter,
                eventBatcher,
                dataProvider,
                bookName,
                stateSessionAlias,
                maxMessageSize
            )

            override fun toString(): String = "transport"
        }

        @JvmStatic
        fun stateStorages() = listOf(
            Arguments.of(PROTOBUF_STATE_STORAGE_PROVIDER, PROTOBUF_MESSAGE_STORAGE_SUPPLIER, PROTOBUF_BATCH_CONVERTER),
            Arguments.of(TRANSPORT_STATE_STORAGE_PROVIDER, TRANSPORT_MESSAGE_STORAGE_SUPPLIER, TRANSPORT_BATCH_CONVERTER)
        )

        // TODO: move to common-utils project
        private fun TransportRawMessage.toProto(book: String, sessionGroup: String): ProtobufRawMessage = ProtobufRawMessage.newBuilder().apply {
            metadataBuilder.apply {
                setId(this@toProto.id.toProto(book, sessionGroup))
                putAllProperties(this@toProto.metadata)
                setProtocol(this@toProto.protocol)
                setBody(UnsafeByteOperations.unsafeWrap(this@toProto.body.toByteArray()))
            }
            this@toProto.eventId?.let {
                setParentEventId(it.toProto())
            }
        }.build()

        private fun createMessageSearchResponse(
            stateType: StateType = SINGLE,
            timestamp: Timestamp = Instant.now().toTimestamp(),
            sequence: Long = 1,
            data: ByteArray = STATE,
        ): MessageSearchResponse =
            MessageSearchResponse.newBuilder().apply {
                messageBuilder.apply {
                    putMessageProperties(METADATA_STATE_TYPE_PROPERTY, stateType.name)
                    messageIdBuilder.apply {
                        this.timestamp = timestamp
                        this.sequence = sequence
                        this.bookName = BOOK_NAME
                        connectionIdBuilder.apply {
                            sessionAlias = STATE_SESSION_ALIAS
                        }
                    }
                    bodyRaw = UnsafeByteOperations.unsafeWrap(data)
                }
            }.build()
    }
}

fun interface StateStorageProvider<T> {
    fun invoke(
        messageRouter: MessageRouter<T>,
        eventBatcher: EventBatcher,
        dataProvider: DataProviderService,
        bookName: String,
        stateSessionAlias: String,
        maxMessageSize: Long
    ): AbstractDataProviderStateStorage<*>
}