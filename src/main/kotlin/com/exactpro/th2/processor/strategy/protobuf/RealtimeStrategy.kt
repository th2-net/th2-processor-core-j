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

package com.exactpro.th2.processor.strategy.protobuf

import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.utils.message.id
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.strategy.AbstractRealtimeStrategy

class RealtimeStrategy(context: Context): AbstractRealtimeStrategy(context) {
    override fun subscribeToMessages(context: Context): SubscriberMonitor {
        return context.commonFactory.messageRouterMessageGroupBatch.subscribe({ _, batch ->
            val exceptionList = mutableListOf<Throwable>()
            batch.groupsList.forEach { messageGroup ->
                messageGroup.messagesList.forEach { anyMessage ->
                    runCatching {
                        when (anyMessage.kindCase) {
                            MESSAGE -> processor.handle(context.processorEventId, anyMessage.message)
                            RAW_MESSAGE -> processor.handle(context.processorEventId, anyMessage.rawMessage)
                            else -> error("Unsupported message kind: ${anyMessage.kindCase}")
                        }
                    }.onFailure { e ->
                        exceptionList.add(e)
                        reportHandleMessageError(anyMessage.id, e)
                    }
                }
            }
            checkAndThrow(exceptionList)
        }, IN_ATTRIBUTE)
    }
}