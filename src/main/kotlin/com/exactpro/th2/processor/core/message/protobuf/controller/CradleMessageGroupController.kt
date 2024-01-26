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

import com.exactpro.th2.common.grpc.AnyMessage
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
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.exactpro.th2.processor.utility.ifTrue
import com.google.protobuf.Timestamp

internal class CradleMessageGroupController(
    private val processor: IProcessor,
    intervalEventId: EventID,
    startTime: Timestamp,
    endTime: Timestamp,
    kinds: Set<MessageKind>,
    bookToGroups: Map<String, Set<String>>
) : Controller<MessageGroupBatch> (
    intervalEventId
) {
    private val cradleMessageGroupState = CradleMessageGroupState(startTime, endTime, kinds.map(MessageKind::grpcKind).toSet(), bookToGroups)

    override val isStateComplete: Boolean
        get() = super.isStateComplete && cradleMessageGroupState.isStateEmpty

    override fun actual(batch: MessageGroupBatch) {
        var needSignal = false
        for (group in batch.groupsList) {
            runCatching {
                val updateResult = updateActualState(batch, group)
                needSignal = needSignal or updateResult
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
        needSignal.ifTrue(::signal)
    }
    override fun expected(loadedStatistic: MessageLoadedStatistic) {
        updateExpectedState(loadedStatistic).ifTrue(::signal)
        super.expected(loadedStatistic)
    }

    override fun expected(loadedStatistic: EventLoadedStatistic): Unit = throw UnsupportedOperationException()

    private fun updateActualState(batch: MessageGroupBatch, group: MessageGroup): Boolean = cradleMessageGroupState.plus(batch, group)

    private fun updateExpectedState(loadedStatistic: MessageLoadedStatistic): Boolean = cradleMessageGroupState.minus(loadedStatistic)

    private fun handle(anyMessage: AnyMessage) {
        when (anyMessage.kindCase) {
            AnyMessage.KindCase.MESSAGE -> processor.handle(intervalEventId, anyMessage.message)
            AnyMessage.KindCase.RAW_MESSAGE -> processor.handle(intervalEventId, anyMessage.rawMessage)
            else -> error("Unsupported message kind: ${anyMessage.kindCase}")
        }
    }
}