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
package com.exactpro.th2.processor.core.state.protobuf

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.processor.core.state.AbstractDataProviderStateStorage
import com.exactpro.th2.processor.core.state.StateType
import com.google.protobuf.UnsafeByteOperations
import java.time.Instant
import javax.annotation.concurrent.NotThreadSafe

@NotThreadSafe
class DataProviderStateStorage(
    messageRouter: MessageRouter<MessageGroupBatch>,
    eventBatcher: EventBatcher,
    dataProvider: DataProviderService,
    bookName: String,
    stateSessionAlias: String,
    maxMessageSize: Long = METADATA_SIZE + MIN_STATE_SIZE
) : AbstractDataProviderStateStorage<MessageGroupBatch>(
    messageRouter, eventBatcher, dataProvider, bookName, stateSessionAlias, maxMessageSize
) {
    override fun MessageGroupBatch.messageIds(): List<MessageID> = groupsList.asSequence()
        .flatMap(MessageGroup::getMessagesList)
        .map(AnyMessage::id)
        .toList()

    override fun ByteArray.createMessageBatch(
        stateTimestamp: Instant,
        stateType: StateType,
        sequence: Long,
    ): MessageGroupBatch = MessageGroupBatch.newBuilder().apply {
        addGroupsBuilder().apply {
            this += RawMessage.newBuilder().apply {
                body = UnsafeByteOperations.unsafeWrap(this@createMessageBatch)
                metadataBuilder.apply {
                    putProperties(StateType.METADATA_STATE_TYPE_PROPERTY, stateType.name)
                    idBuilder.apply {
                        this.bookName = book
                        this.timestamp = stateTimestamp.toTimestamp()
                        this.direction = Direction.SECOND
                        this.sequence = sequence
                        connectionIdBuilder.apply {
                            this.sessionAlias = stateSessionAlias
                            this.sessionGroup = stateSessionAlias
                        }
                    }
                }
            }
        }
    }.build()
}