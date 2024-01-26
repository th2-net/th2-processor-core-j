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

package com.exactpro.th2.processor.core.message.transport.controller

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.HandleMessageException
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.exactpro.th2.processor.utility.ifTrue
import java.time.Instant

internal class CradleMessageGroupController(
    private val processor: IProcessor,
    intervalEventId: EventID,
    startTime: Instant,
    endTime: Instant,
    kinds: Set<MessageKind>,
    bookToGroups: Map<String, Set<String>>
) : Controller<GroupBatch>(
    intervalEventId
) {
    private val cradleMessageGroupState = CradleMessageGroupState(startTime, endTime, kinds, bookToGroups)

    override val isStateComplete: Boolean
        get() = super.isStateComplete && cradleMessageGroupState.isStateEmpty

    override fun actual(batch: GroupBatch) {
        var needSignal = false
        for (group in batch.groups) {
            runCatching {
                val updateResult = updateActualState(batch, group)
                needSignal = needSignal or updateResult
                for (anyMessage in group.messages) {
                    runCatching {
                        updateLastProcessed(anyMessage.id.timestamp)
                        handle(anyMessage)
                    }.onFailure { e ->
                        throw HandleMessageException(batch.book, batch.sessionGroup, listOf(anyMessage.id), cause = e)
                    }
                }
            }.onFailure { e ->
                throw HandleMessageException(batch.book, batch.sessionGroup, group.messages.map(Message<*>::id), cause = e)
            }
        }
        needSignal.ifTrue(::signal)
    }
    override fun expected(loadedStatistic: MessageLoadedStatistic) {
        updateExpectedState(loadedStatistic).ifTrue(::signal)
        super.expected(loadedStatistic)
    }

    override fun expected(loadedStatistic: EventLoadedStatistic): Unit = throw UnsupportedOperationException()

    private fun updateActualState(batch: GroupBatch, group: MessageGroup): Boolean = cradleMessageGroupState.plus(batch, group)

    private fun updateExpectedState(loadedStatistic: MessageLoadedStatistic): Boolean = cradleMessageGroupState.minus(loadedStatistic)

    private fun handle(message: Message<*>) {
        when (message) {
            is ParsedMessage -> processor.handle(intervalEventId, message)
            is RawMessage -> processor.handle(intervalEventId, message)
            else -> error("Unsupported message kind: ${message::class.java}")
        }
    }
}