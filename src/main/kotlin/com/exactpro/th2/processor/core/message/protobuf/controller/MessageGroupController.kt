/*
 *  Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.processor.core.message.protobuf.controller

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.common.utils.message.timestamp
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.HandleMessageException
import com.exactpro.th2.processor.core.state.StateUpdater
import com.exactpro.th2.processor.utility.ifTrue
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
internal abstract class MessageGroupController(
    private val processor: IProcessor,
    intervalEventId: EventID
) : Controller<MessageGroupBatch>(
    intervalEventId
) {
    override fun actual(batch: MessageGroupBatch) {
        updateActualState {
            for (group in batch.groupsList) {
                runCatching {
                    updateState(batch, group)
                    for (anyMessage in group.messagesList) {
                        runCatching {
                            updateLastProcessed(anyMessage.timestamp.toInstant())
                            handle(anyMessage)
                        }.onFailure { e ->
                            throw HandleMessageException(listOf(anyMessage.id), cause = e)
                        }
                    }
                }.onFailure { e ->
                    throw HandleMessageException(group.messagesList.map { it.id }, cause = e)
                }
            }
        }.ifTrue(::signal)
    }
    override fun expected(loadedStatistic: MessageLoadedStatistic) {
        updateExpectedState(loadedStatistic).ifTrue(::signal)
        super.expected(loadedStatistic)
    }

    override fun expected(loadedStatistic: EventLoadedStatistic) {
        throw UnsupportedOperationException()
    }
    protected abstract fun updateActualState(func: StateUpdater<MessageGroupBatch, MessageGroup>.() -> Unit): Boolean
    protected abstract fun updateExpectedState(loadedStatistic: MessageLoadedStatistic): Boolean

    private fun handle(anyMessage: AnyMessage) {
        when (anyMessage.kindCase) {
            MESSAGE -> processor.handle(intervalEventId, anyMessage.message)
            RAW_MESSAGE -> processor.handle(intervalEventId, anyMessage.rawMessage)
            else -> error("Unsupported message kind: ${anyMessage.kindCase}")
        }
    }
}

