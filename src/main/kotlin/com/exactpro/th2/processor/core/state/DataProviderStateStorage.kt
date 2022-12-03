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

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation.PREVIOUS
import com.exactpro.th2.processor.core.state.StateType.Companion.METADATA_STATE_TYPE_PROPERTY
import com.exactpro.th2.processor.core.state.StateType.END
import com.exactpro.th2.processor.core.state.StateType.INTERMEDIATE
import com.exactpro.th2.processor.core.state.StateType.SINGLE
import com.exactpro.th2.processor.core.state.StateType.START
import com.google.protobuf.Int32Value
import com.google.protobuf.Timestamp
import com.google.protobuf.UnsafeByteOperations
import com.google.protobuf.util.Timestamps
import com.google.protobuf.util.Timestamps.toString
import mu.KotlinLogging
import java.time.Instant
import javax.annotation.concurrent.NotThreadSafe

@NotThreadSafe
class DataProviderStateStorage(
    private val messageRouter: MessageRouter<MessageGroupBatch>,
    private val dataProvider: DataProviderService,
    private val stateSessionAlias: String,
    maxMessageSize: Long = METADATA_SIZE + MIN_STATE_SIZE
) : IStateStorage {

    init {
        check(maxMessageSize - METADATA_SIZE >= MIN_STATE_SIZE) {
            "`Max message size` too small, because max message size ($maxMessageSize) - metadata size ($PROPERTY_SIZE) < min state size $MIN_STATE_SIZE"
        }
    }

    private val stateSize = maxMessageSize - METADATA_SIZE
    private var sequenceCounter: Long = System.nanoTime()

    private val requestBuilder = MessageSearchRequest.newBuilder().apply {
        searchDirection = PREVIOUS
        resultCountLimit = Int32Value.of(COUNT_LIMIT)
        addResponseFormats(RAW_MESSAGE_RESPONSE_FORMAT)
        addStreamBuilder().apply {
            name = stateSessionAlias
            direction = Direction.SECOND
        }
    }

    override fun loadState(): ByteArray? {
        K_LOGGER.info { "Started state loading" }

        val state = mutableListOf<Pair<StateType, MessageGroupResponse>>()
        sequence {
            var iterator = dataProvider.searchMessages(createSearchRequest().also {
                K_LOGGER.info { "Request to load state ${it.toJson()}" }
            })

            while (iterator.hasNext()) {
                val message: MessageGroupResponse = iterator.next().message
                yield(message)
                if (!iterator.hasNext()) {
                    iterator = dataProvider.searchMessages(createSearchRequest(message.messageId.timestamp).also {
                        K_LOGGER.info { "Request to load state ${it.toJson()}" }
                    })
                }
            }
        }.map(::mapStateTypeToMessage)
        .dropWhile { (stateType, _) -> !(stateType == SINGLE || stateType == END) }
        .forEach { pair ->
            val (stateType, message) = pair
            K_LOGGER.debug { "Process raw message ${message.toJson()}" }
            when (stateType) {
                SINGLE -> return message.bodyRaw.toByteArray()
                END, INTERMEDIATE, START -> {
                    if (state.checkAndModify(pair)) {
                        val list: List<Byte> =
                            state.reversed().flatMap { (_, message) -> message.bodyRaw.toByteArray().asSequence() }
                        return ByteArray(list.size).apply { list.forEachIndexed(this::set) }
                    }
                }
                else -> {
                    K_LOGGER.warn { "Drop broken state ${state.reversed().logState} and skipped unknown message ${pair.log}" }
                    state.clear()
                }
            }
        }
        return null
    }


    override fun saveState(state: ByteArray) {
        val stateTimestamp = Instant.now().toTimestamp()
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
                messageRouter.sendAll(bytes.toByteArray().createMessageBatch(
                    stateTimestamp,
                    stateType,
                    sequenceCounter++,
                    stateSessionAlias
                ))
            }
        } else {
            messageRouter.sendAll(state.createMessageBatch(
                stateTimestamp,
                SINGLE,
                sequenceCounter++,
                stateSessionAlias
            ))
        }
    }

    private fun createSearchRequest(timestamp: Timestamp = Instant.now().toTimestamp()): MessageSearchRequest =
        requestBuilder.apply {
            startTimestamp = timestamp
        }.build()

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }
        private val PROPERTY_SIZE: Long = (METADATA_STATE_TYPE_PROPERTY.length + StateType.values().maxOf { it.name.length }).toLong()

        private const val RAW_MESSAGE_RESPONSE_FORMAT = "BASE_64"
        private const val COUNT_LIMIT = 100

        const val MIN_STATE_SIZE = 256 * 1_024
        val METADATA_SIZE = PROPERTY_SIZE

        private fun ByteArray.createMessageBatch(
            stateTimestamp: Timestamp,
            stateType: StateType,
            sequence: Long,
            sessionAlias: String,
        ): MessageGroupBatch = MessageGroupBatch.newBuilder().apply {
            addGroupsBuilder().apply {
                this += RawMessage.newBuilder().apply {
                    body = UnsafeByteOperations.unsafeWrap(this@createMessageBatch)
                    metadataBuilder.apply {
                        putProperties(METADATA_STATE_TYPE_PROPERTY, stateType.name)
                        idBuilder.apply {
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
        private fun MutableList<Pair<StateType, MessageGroupResponse>>.checkAndModify(pair: Pair<StateType, MessageGroupResponse>): Boolean {
            val (statusType, message) = pair
            when(statusType) {
                START, INTERMEDIATE -> {
                    if (isEmpty()) {
                        K_LOGGER.warn { "Reversed state can't be start from ${pair.log} message" }
                    } else {
                        val (_, lastMessage) = last()
                        if (message.messageId.sequence + 1 != lastMessage.messageId.sequence) {
                            K_LOGGER.warn { "Dropped broken by sequence state ${reversed().logState}, current message ${pair.log}" }
                            clear()
                            return false
                        }
                        if (Timestamps.compare(message.messageId.timestamp, lastMessage.messageId.timestamp) != 0) {
                            K_LOGGER.warn { "Dropped broken by timestamp state ${reversed().logState}, current message ${pair.log}" }
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
                        K_LOGGER.warn { "Dropped uncompleted state ${reversed().logState}, current message ${pair.log}" }
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

        private val List<Pair<StateType, MessageGroupResponse>>.logState: String
            get() = joinToString(",", "[", "]") { pair -> pair.log }

        private val Pair<StateType, MessageGroupResponse>.log: String
            get() = "${toString(second.messageId.timestamp)} - ${second.messageId.logId} ($first)"
    }
}