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
package com.exactpro.th2.processor.core.state

import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.bean.IRow
import com.exactpro.th2.common.event.bean.Table
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.common.utils.state.StateManager
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.processor.core.state.StateType.Companion.METADATA_STATE_TYPE_PROPERTY
import com.exactpro.th2.processor.core.state.StateType.END
import com.exactpro.th2.processor.core.state.StateType.INTERMEDIATE
import com.exactpro.th2.processor.core.state.StateType.SINGLE
import com.exactpro.th2.processor.core.state.StateType.START
import com.exactpro.th2.processor.utility.log
import com.google.protobuf.Timestamp
import com.google.protobuf.UnsafeByteOperations
import com.google.protobuf.util.Timestamps
import com.google.protobuf.util.Timestamps.toString
import mu.KotlinLogging
import java.time.Instant
import javax.annotation.concurrent.NotThreadSafe

typealias EventBuilder = com.exactpro.th2.common.event.Event

@NotThreadSafe
class DataProviderStateManager(
    private val onEvent: (event: Event) -> Unit,
    private val loadRawMessages: (bookName: String, sessionAlias: String, timestamp: Timestamp) -> Iterator<MessageSearchResponse>,
    private val storeRawMessage: (statePart: MessageGroupBatch) -> Unit,
    private val stateSessionAlias: String,
    maxMessageSize: Long = METADATA_SIZE + MIN_STATE_SIZE
) : StateManager {

    init {
        check(maxMessageSize - METADATA_SIZE >= MIN_STATE_SIZE) {
            "`Max message size` too small, because max message size ($maxMessageSize) - metadata size ($PROPERTY_SIZE) < min state size $MIN_STATE_SIZE"
        }
    }

    private val stateSize = maxMessageSize - METADATA_SIZE
    private var sequenceCounter: Long = System.nanoTime()

    override fun load(parentEventId: EventID, stateSessionAlias: String, bookName: String): ByteArray? {
        K_LOGGER.info { "Started state loading" }

        val state = mutableListOf<Pair<StateType, MessageGroupResponse>>()
        sequence {
            var iterator = loadRawMessages(bookName, stateSessionAlias, Instant.now().toTimestamp())

            while (iterator.hasNext()) {
                val message: MessageGroupResponse = iterator.next().message
                yield(message)
                if (!iterator.hasNext()) {
                    iterator = loadRawMessages(bookName, stateSessionAlias, message.messageId.timestamp)
                }
            }
        }.map(::mapStateTypeToMessage)
            .dropWhile { (stateType, _) -> !(stateType == SINGLE || stateType == END) }
            .forEach { pair ->
                val (stateType, response) = pair
                K_LOGGER.debug { "Process raw response ${response.toJson()}" }
                when (stateType) {
                    SINGLE -> {
                        reportStateLoaded(parentEventId, pair)
                        return response.bodyRaw.toByteArray()
                    }
                    END, INTERMEDIATE, START -> {
                        if (state.checkAndModify(pair)) {
                            val list: List<Byte> =
                                state.reversed().flatMap { (_, message) -> message.bodyRaw.toByteArray().asSequence() }
                            reportStateLoaded(parentEventId, state)
                            return ByteArray(list.size).apply { list.forEachIndexed(this::set) }
                        }
                    }
                    else -> {
                        reportSkipState(parentEventId, state, pair)
                        state.clear()
                    }
                }
            }
        reportEmptyStateLoaded(parentEventId)
        return null
    }

    override fun store(parentEventId: EventID, state: ByteArray, stateSessionAlias: String, bookName: String) {
        val stateTimestamp = Instant.now().toTimestamp()
        val batches = mutableListOf<MessageGroupBatch>()
        if (state.size > stateSize) {
            val parts = state.asSequence()
                .chunked(stateSize.toInt()) //FIXME: implement another approach supported long
                .toList()

            parts.forEachIndexed { index, bytes ->
                val stateType = when(index) {
                    0               -> START
                    parts.lastIndex -> END
                    else            -> INTERMEDIATE
                }
                batches.add(bytes.toByteArray().createMessageBatch(
                    stateTimestamp,
                    stateType,
                    sequenceCounter++,
                    bookName,
                    stateSessionAlias
                ))
            }
        } else {
            batches.add(state.createMessageBatch(
                stateTimestamp,
                SINGLE,
                sequenceCounter++,
                bookName,
                stateSessionAlias
            ))
        }
        reportStateSaved(parentEventId, batches)

        batches.forEach { batch -> storeRawMessage(batch) }
    }

    private fun reportEmptyStateLoaded(processorEventId: EventID) {
        onEvent(EventBuilder.start().apply {
            name("State not found or broken in $stateSessionAlias session alias")
            type(TYPE_LOAD_STATE)
        }.toProto(processorEventId)
            .log(K_LOGGER))
    }

    private fun reportSkipState(
        processorEventId: EventID,
        state: List<Pair<StateType, MessageGroupResponse>>,
        pair: Pair<StateType, MessageGroupResponse>
    ) {
        onEvent(EventBuilder.start().apply {
            name("Drop broken state with size ${state.size} and skipped unknown message ${pair.log}")
            type(TYPE_LOAD_STATE)
            status(FAILED)
            bodyData(state.toTable("Broken state"))
            bodyData(pair.toTable("Unknown state type"))
            state.forEach { (_, response) -> messageID(response.messageId) }
        }.toProto(processorEventId)
            .log(K_LOGGER))
    }

    private fun reportStateLoaded(processorEventId: EventID, pair: Pair<StateType, MessageGroupResponse>) {
        reportStateLoaded(processorEventId, listOf(pair))
    }
    private fun reportStateLoaded(processorEventId: EventID, state: List<Pair<StateType, MessageGroupResponse>>) {
        onEvent(EventBuilder.start().apply {
            name("Loaded state from ${state.size} messages")
            type(TYPE_LOAD_STATE)
            bodyData(state.toTable("State"))
            state.forEach { (_, response) -> messageID(response.messageId) }
        }.toProto(processorEventId)
            .log(K_LOGGER))
    }

    private fun reportStateSaved(intervalEventId: EventID, batches: MutableList<MessageGroupBatch>) {
        onEvent(EventBuilder.start().apply {
            var messages = 0
            batches.forEach { batch ->
                batch.groupsList.asSequence()
                    .flatMap(MessageGroup::getMessagesList)
                    .forEach { anyMessage ->
                        messages++
                        messageID(anyMessage.id)
                    }
            }
            name("Published state via $messages messages in ${batches.size} batches")
            type("Save state")
        }.toProto(intervalEventId)
            .log(K_LOGGER))
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
        private val PROPERTY_SIZE: Long = (METADATA_STATE_TYPE_PROPERTY.length + StateType.values().maxOf { it.name.length }).toLong()

        private const val TYPE_LOAD_STATE = "Load state"

        const val MIN_STATE_SIZE = 256 * 1_024
        val METADATA_SIZE = PROPERTY_SIZE

        private fun ByteArray.createMessageBatch(
            stateTimestamp: Timestamp,
            stateType: StateType,
            sequence: Long,
            bookName: String,
            sessionAlias: String,
        ): MessageGroupBatch = MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    body = UnsafeByteOperations.unsafeWrap(this@createMessageBatch)
                    metadataBuilder.apply {
                        putProperties(METADATA_STATE_TYPE_PROPERTY, stateType.name)
                        idBuilder.apply {
                            this.bookName = bookName
                            this.timestamp = stateTimestamp
                            this.direction = Direction.SECOND
                            this.sequence = sequence
                            connectionIdBuilder.apply {
                                this.sessionAlias = sessionAlias
                            }
                        }
                    }
                }
            }
        }.build()
        private fun MutableList<Pair<StateType, MessageGroupResponse>>.checkAndModify(
            pair: Pair<StateType, MessageGroupResponse>
        ): Boolean {
            val (statusType, message) = pair
            when(statusType) {
                START, INTERMEDIATE -> {
                    if (isEmpty()) {
                        K_LOGGER.warn { "Reversed state can't be start from ${pair.log} message" } //FIXME: publish as event
                    } else {
                        val (_, lastMessage) = last()
                        if (message.messageId.sequence + 1 != lastMessage.messageId.sequence) {
                            K_LOGGER.warn {
                                "Dropped broken by sequence state ${reversed().logState}, current message ${pair.log}"
                            } //FIXME: publish as event
                            clear()
                            return false
                        }
                        if (Timestamps.compare(message.messageId.timestamp, lastMessage.messageId.timestamp) != 0) {
                            K_LOGGER.warn {
                                "Dropped broken by timestamp state ${reversed().logState}, current message ${pair.log}"
                            } //FIXME: publish as event
                            clear()
                            return false
                        }

                        add(pair)
                        if (statusType == START) {
                            return true
                        }
                    }
                }
                END -> {
                    if (isNotEmpty()) {
                        K_LOGGER.warn {
                            "Dropped uncompleted state ${reversed().logState}, current message ${pair.log}"
                        } //FIXME: publish as event
                        clear()
                    }
                    add(pair)
                    return false
                }
                else -> error("Unsupported state type ${pair.log}")
            }
            return false
        }

        private fun mapStateTypeToMessage(message: MessageGroupResponse) =
            StateType.parse(message.messagePropertiesMap[METADATA_STATE_TYPE_PROPERTY]) to message

        private fun List<Pair<StateType, MessageGroupResponse>>.toTable(tableType: String): Table = Table().apply {
            type = tableType
            fields = map { (type, report) ->
                StateRow(type, report.messageId.logId)
            }
        }
        private fun Pair<StateType, MessageGroupResponse>.toTable(tableType: String): Table = Table().apply {
            type = tableType
            fields = listOf(
                StateRow(first, second.messageId.logId)
            )
        }
        private val List<Pair<StateType, MessageGroupResponse>>.logState: String
            get() = joinToString(",", "[", "]") { pair -> pair.log }

        private val Pair<StateType, MessageGroupResponse>.log: String
            get() = "${toString(second.messageId.timestamp)} - ${second.messageId.logId} ($first)"

        private data class StateRow(val type: StateType, val messageId: String): IRow
    }
}