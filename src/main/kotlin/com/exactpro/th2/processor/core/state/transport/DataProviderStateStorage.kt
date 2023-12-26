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
package com.exactpro.th2.processor.core.state.transport

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.transport.toBatch
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.exactpro.th2.common.utils.message.transport.toProto
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.processor.core.state.AbstractDataProviderStateStorage
import com.exactpro.th2.processor.core.state.StateType
import java.time.Instant
import javax.annotation.concurrent.NotThreadSafe

@NotThreadSafe
class DataProviderStateStorage(
    messageRouter: MessageRouter<GroupBatch>,
    eventBatcher: EventBatcher,
    dataProvider: DataProviderService,
    bookName: String,
    stateSessionAlias: String,
    maxMessageSize: Long = METADATA_SIZE + MIN_STATE_SIZE
) : AbstractDataProviderStateStorage<GroupBatch>(
    messageRouter, eventBatcher, dataProvider, bookName, stateSessionAlias, maxMessageSize
) {
    override fun GroupBatch.messageIds(): List<MessageID> = groups.asSequence()
        .flatMap(MessageGroup::messages)
        .map { it.id.toProto(this@DataProviderStateStorage.book, stateSessionAlias) }
        .toList()

    override fun ByteArray.createMessageBatch(
        stateTimestamp: Instant,
        stateType: StateType,
        sequence: Long,
    ): GroupBatch = RawMessage.builder().apply {
        setBody(this@createMessageBatch)
        idBuilder().apply { 
            setTimestamp(stateTimestamp)
            setDirection(Direction.OUTGOING)
            setSequence(sequence)
            setSessionAlias(stateSessionAlias)
        }
        metadataBuilder().apply { 
            put(StateType.METADATA_STATE_TYPE_PROPERTY, stateType.name)
        }
    }.build().toGroup().toBatch(book, stateSessionAlias)
}