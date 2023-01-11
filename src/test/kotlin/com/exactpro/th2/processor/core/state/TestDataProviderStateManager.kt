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

package com.exactpro.th2.processor.core.state

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.processor.core.state.DataProviderStateManager.Companion.METADATA_SIZE
import com.exactpro.th2.processor.core.state.DataProviderStateManager.Companion.MIN_STATE_SIZE
import com.exactpro.th2.processor.core.state.StateType.Companion.METADATA_STATE_TYPE_PROPERTY
import com.exactpro.th2.processor.core.state.StateType.END
import com.exactpro.th2.processor.core.state.StateType.INTERMEDIATE
import com.exactpro.th2.processor.core.state.StateType.SINGLE
import com.exactpro.th2.processor.core.state.StateType.START
import com.google.protobuf.Timestamp
import com.google.protobuf.UnsafeByteOperations
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.time.Instant
import kotlin.math.min
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

internal class TestDataProviderStateManager {

    private val eventBatcher = mock<EventBatcher> {  }

    @Test
    fun `max message size argument`() {
        assertDoesNotThrow {
            DataProviderStateManager(
                onEvent = { },
                loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
                storeRawMessage = {  },
                STATE_SESSION_ALIAS,
                MIN_STATE_SIZE + METADATA_SIZE
            )
        }

        assertFailsWith<IllegalStateException> {
            DataProviderStateManager(
                onEvent = { },
                loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
                storeRawMessage = {  },
                STATE_SESSION_ALIAS,
                MIN_STATE_SIZE + METADATA_SIZE - 1
            )
        }
    }

    @Test
    fun `load state from unknown alias`() {
        val responseIterator = sequence {
            while (true) {
                yield(emptyList<MessageSearchResponse>().iterator())
            }

        }.iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { responseIterator.next() }
        }

        val storage = DataProviderStateManager(
            onEvent = { },
            loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
            storeRawMessage = {  },
            STATE_SESSION_ALIAS,
            MIN_STATE_SIZE + METADATA_SIZE
        )
        assertNull(storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Load empty state")
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @Test
    fun `load state from single message`() {
        val responseIterator = sequence {
            yield(listOf(createMessageSearchResponse()).iterator())
            while (true) {
                yield(emptyList<MessageSearchResponse>().iterator())
            }

        }.iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { responseIterator.next() }
        }

        val storage = DataProviderStateManager(
            onEvent = { },
            loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
            storeRawMessage = { },
            STATE_SESSION_ALIAS,
            MIN_STATE_SIZE + METADATA_SIZE
        )

        assertArrayEquals(STATE, storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Load state")
        verify(dataProvider, times(1).description("Number of search messages calls")).searchMessages(any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @Test
    fun `load state from multiple message`() {
        val parts = 3
        check(parts > 1) {
            "Number of parts is less than 1"
        }

        val responseIterator = sequence {
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
                yield(
                    listOf(
                        createMessageSearchResponse(
                            stateType,
                            timestamp,
                            revertedIndex.toLong() + 1,
                            data
                        )
                    ).iterator()
                )
            }
            while (true) {
                yield(emptyList<MessageSearchResponse>().iterator())
            }
        }.iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { responseIterator.next() }
        }

        val storage = DataProviderStateManager(
                onEvent = { },
                loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
                storeRawMessage = {  },
                STATE_SESSION_ALIAS,
                MIN_STATE_SIZE + METADATA_SIZE
            )
        assertArrayEquals(STATE, storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Load state")
        verify(dataProvider, times(parts).description("Number of search messages calls")).searchMessages(any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @Test
    fun `load state state after skip uncompleted state`() {
        val responseIterator = sequence {
            val now = Instant.now()
            now.plusNanos(5).toTimestamp().also { timestamp ->
                yield(listOf(createMessageSearchResponse(END, timestamp, 51, STATE)).iterator())
            }

            now.plusNanos(4).toTimestamp().also { timestamp ->
                yield(listOf(createMessageSearchResponse(INTERMEDIATE, timestamp, 41, STATE)).iterator())
            }

            now.plusNanos(3).toTimestamp().also { timestamp ->
                yield(listOf(createMessageSearchResponse(START, timestamp, 31, STATE)).iterator())
            }

            now.plusNanos(2).toTimestamp().also { timestamp ->
                yield(listOf(createMessageSearchResponse(END, timestamp, 22, STATE)).iterator())
                yield(listOf(createMessageSearchResponse(INTERMEDIATE, timestamp, 21, STATE)).iterator())
            }

            now.plusNanos(1).toTimestamp().also { timestamp ->
                yield(listOf(createMessageSearchResponse(INTERMEDIATE, timestamp, 12, STATE)).iterator())
                yield(listOf(createMessageSearchResponse(START, timestamp, 11, STATE)).iterator())
            }

            yield(listOf(createMessageSearchResponse(SINGLE, now.toTimestamp(), 1, STATE)).iterator())
            while (true) {
                yield(emptyList<MessageSearchResponse>().iterator())
            }

        }.iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { responseIterator.next() }
        }

        val storage = DataProviderStateManager(
            onEvent = { },
            loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
            storeRawMessage = {  },
            STATE_SESSION_ALIAS,
            MIN_STATE_SIZE + METADATA_SIZE
        )
        assertArrayEquals(STATE, storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Load state")
        verify(dataProvider, times(8).description("Number of search messages calls")).searchMessages(any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @Test
    fun `load state state after skip boken state by timestamp`() {
        val responseIterator = sequence {
            Instant.now().also { now ->
                val earlierTimestamp = now.minusNanos(1).toTimestamp()
                val nowTimestamp = now.toTimestamp()
                val laterTimestamp = now.plusNanos(1).toTimestamp()

                yield(listOf(createMessageSearchResponse(END, laterTimestamp, 8, STATE)).iterator())
                yield(listOf(createMessageSearchResponse(START, nowTimestamp, 7, STATE)).iterator())

                yield(listOf(createMessageSearchResponse(END, laterTimestamp, 6, STATE)).iterator())
                yield(listOf(createMessageSearchResponse(INTERMEDIATE, nowTimestamp, 5, STATE)).iterator())
                yield(listOf(createMessageSearchResponse(START, nowTimestamp, 4, STATE)).iterator())

                yield(listOf(createMessageSearchResponse(END, nowTimestamp, 3, STATE)).iterator())
                yield(listOf(createMessageSearchResponse(START, earlierTimestamp, 2, STATE)).iterator())

                yield(listOf(createMessageSearchResponse(SINGLE, nowTimestamp, 1, STATE)).iterator())
            }

            while (true) {
                yield(emptyList<MessageSearchResponse>().iterator())
            }

        }.iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { responseIterator.next() }
        }

        val storage = DataProviderStateManager(
            onEvent = { },
            loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
            storeRawMessage = {  },
            STATE_SESSION_ALIAS,
            MIN_STATE_SIZE + METADATA_SIZE
        )
        assertArrayEquals(STATE, storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Load state")
        verify(dataProvider, times(8).description("Number of search messages calls")).searchMessages(any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @Test
    fun `load state state after skip boken state by sequence`() {
        val responseIterator = sequence {
            val timestamp = Instant.now().toTimestamp()
            yield(listOf(createMessageSearchResponse(END, timestamp, 12, STATE)).iterator()) // Gap
            yield(listOf(createMessageSearchResponse(INTERMEDIATE, timestamp, 10, STATE)).iterator())
            yield(listOf(createMessageSearchResponse(START, timestamp, 8, STATE)).iterator())

            yield(listOf(createMessageSearchResponse(END, timestamp, 8, STATE)).iterator())
            yield(listOf(createMessageSearchResponse(INTERMEDIATE, timestamp, 7, STATE)).iterator()) // Gap
            yield(listOf(createMessageSearchResponse(START, timestamp, 5, STATE)).iterator())

            yield(listOf(createMessageSearchResponse(END, timestamp, 4, STATE)).iterator()) // Gap
            yield(listOf(createMessageSearchResponse(START, timestamp, 2, STATE)).iterator())

            yield(listOf(createMessageSearchResponse(SINGLE, timestamp, 1, STATE)).iterator())

            while (true) {
                yield(emptyList<MessageSearchResponse>().iterator())
            }

        }.iterator()

        val dataProvider: DataProviderService = mock {
            on { searchMessages(any()) }.thenAnswer { responseIterator.next() }
        }

        val storage = DataProviderStateManager(
            onEvent = { },
            loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
            storeRawMessage = {  },
            STATE_SESSION_ALIAS,
            MIN_STATE_SIZE + METADATA_SIZE
        )
        assertArrayEquals(STATE, storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Load state")
        verify(dataProvider, times(9).description("Number of search messages calls")).searchMessages(any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @Test
    fun `save state to single message`() {
        val messageRouter: MessageRouter<MessageGroupBatch> = mock { }
        val data = ByteArray(MIN_STATE_SIZE)

        val storage = DataProviderStateManager(
            onEvent = { },
            loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
            storeRawMessage = {  },
            STATE_SESSION_ALIAS,
            MIN_STATE_SIZE + METADATA_SIZE
        )
        storage.store(EVENT_ID, data, STATE_SESSION_ALIAS, BOOK_NAME)
        verify(messageRouter, times(1).description("State parts")).sendAll(any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @Test
    fun `save state to multiple messages`() {
        val messageRouter: MessageRouter<MessageGroupBatch> = mock { }
        val parts = 3
        val data = ByteArray(parts * MIN_STATE_SIZE).apply(Random::nextBytes)

        val storage = DataProviderStateManager(
            onEvent = { },
            loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
            storeRawMessage = {  },
            STATE_SESSION_ALIAS,
            MIN_STATE_SIZE + METADATA_SIZE
        )
        storage.store(EVENT_ID, data, STATE_SESSION_ALIAS, BOOK_NAME)
        verify(messageRouter, times(parts).description("State parts")).sendAll(any())
        verify(eventBatcher, times(1).description("Publish events")).onEvent(any())
    }

    @Test
    fun `save and load state`() {
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
        val messageRouter: MessageRouter<MessageGroupBatch> = mock {
            on { sendAll(any()) }.then { invocation ->
                cache.add(invocation.arguments[0] as MessageGroupBatch)
            }
        }
        val storage = DataProviderStateManager(
            onEvent = { },
            loadRawMessages = { bookName, sessionAlias, timestamp -> listOf<MessageSearchResponse>().iterator() },
            storeRawMessage = {  },
            STATE_SESSION_ALIAS,
            MIN_STATE_SIZE + METADATA_SIZE
        )

        val singleData = ByteArray(MIN_STATE_SIZE).apply(Random::nextBytes)
        storage.store(EVENT_ID, singleData, STATE_SESSION_ALIAS, BOOK_NAME)
        verify(messageRouter, times(1).description("Single state is published as single raw message"))
            .sendAll(argThat { batch -> batch.verifyBatch(SINGLE, singleData) })
        assertEquals(1, cache.size, "Single state in cache")
        assertArrayEquals(singleData, storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Loaded single data")
        verify(dataProvider, times(1).description("Number of search messages calls")).searchMessages(any())

        val doubleData = ByteArray(MIN_STATE_SIZE * 2).apply(Random::nextBytes)
        storage.store(EVENT_ID, doubleData, STATE_SESSION_ALIAS, BOOK_NAME)
        verify(messageRouter, times(1).description("First part of double state is published as start raw message"))
            .sendAll(argThat { batch -> batch.verifyBatch(START, doubleData.copyOfRange(0, MIN_STATE_SIZE)) })
        verify(messageRouter, times(1).description("Second part of double state is published as start raw message"))
            .sendAll(argThat { batch -> batch.verifyBatch(END, doubleData.copyOfRange(MIN_STATE_SIZE, MIN_STATE_SIZE * 2)) })
        assertEquals(3, cache.size, "Double state in cache")
        assertArrayEquals(doubleData, storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Loaded double data")
        verify(dataProvider, times(2).description("Number of search messages calls")).searchMessages(any())

        val tripleData = ByteArray(MIN_STATE_SIZE * 3).apply(Random::nextBytes)
        storage.store(EVENT_ID, tripleData, STATE_SESSION_ALIAS, BOOK_NAME)
        verify(messageRouter, times(1).description("First part of triple state is published as start raw message"))
            .sendAll(argThat { batch -> batch.verifyBatch(START, tripleData.copyOfRange(0, MIN_STATE_SIZE)) })
        verify(messageRouter, times(1).description("Second part of triple state is published as start raw message"))
            .sendAll(argThat { batch -> batch.verifyBatch(INTERMEDIATE, tripleData.copyOfRange(MIN_STATE_SIZE, MIN_STATE_SIZE * 2)) })
        verify(messageRouter, times(1).description("Third part of triple state is published as start raw message"))
            .sendAll(argThat { batch -> batch.verifyBatch(END, tripleData.copyOfRange(MIN_STATE_SIZE * 2, MIN_STATE_SIZE * 3)) })
        assertEquals(6, cache.size, "Triple state in cache")
        assertArrayEquals(tripleData, storage.load(EVENT_ID, STATE_SESSION_ALIAS, BOOK_NAME), "Loaded triple data")
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

        private val DUMMY_MESSAGE_ROUTER: MessageRouter<MessageGroupBatch> = mock { }
        private val DUMMY_DATA_PROVIDER: DataProviderService = mock { }

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