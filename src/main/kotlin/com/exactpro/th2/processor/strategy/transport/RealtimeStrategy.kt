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

package com.exactpro.th2.processor.strategy.transport

import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.message.transport.toProto
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.strategy.AbstractRealtimeStrategy

class RealtimeStrategy(context: Context): AbstractRealtimeStrategy(context) {
    override fun subscribeToMessages(context: Context): SubscriberMonitor =
        context.commonFactory.transportGroupBatchRouter.subscribe({ _, batch ->
            val exceptionList = mutableListOf<Throwable>()
            batch.groups.forEach { messageGroup ->
                messageGroup.messages.forEach { message ->
                    runCatching {
                        when (message) {
                            is ParsedMessage -> processor.handle(context.processorEventId, message)
                            is RawMessage -> processor.handle(context.processorEventId, message)
                            else -> error("Unsupported message kind: ${message::class.java}")
                        }
                    }.onFailure { e ->
                        exceptionList.add(e)
                        reportHandleMessageError(message.id.toProto(batch), e)
                    }
                }
            }
            checkAndThrow(exceptionList)
        }, IN_ATTRIBUTE)
}