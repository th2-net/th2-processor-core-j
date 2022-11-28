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

package com.exactpro.th2.processor.core.message.controller

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.message.controller.state.StateUpdater
import com.exactpro.th2.processor.utility.ifTrue
import com.exactpro.th2.processor.utility.timestamp
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
internal abstract class MessageController(
    private val processor: IProcessor,
    protected val kind: AnyMessage.KindCase
) : Controller<MessageGroupBatch>() {
    override fun actual(batch: MessageGroupBatch) {
        updateState {
            for (group in batch.groupsList) {
                for (anyMessage in group.messagesList) {
                    updateState(anyMessage)

                    // TODO: refactor looks strange
                    updateLastProcessed(anyMessage.timestamp)
                    handle(anyMessage)
                }
            }
        }.ifTrue(::signal)
    }
    protected abstract fun updateState(func: StateUpdater.() -> Unit): Boolean

    private fun handle(anyMessage: AnyMessage) {
        when (kind) {
            MESSAGE -> processor.handle(anyMessage.message)
            RAW_MESSAGE -> processor.handle(anyMessage.rawMessage)
            else -> error("Unsupported message kind: $kind")
        }
    }
}

