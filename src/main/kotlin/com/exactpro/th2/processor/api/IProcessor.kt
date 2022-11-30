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

package com.exactpro.th2.processor.api

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.RawMessage
import javax.annotation.concurrent.ThreadSafe

@ThreadSafe
interface IProcessor : AutoCloseable {
    fun handle(intervalEventId: EventID, message: Message) {
        throw UnsupportedOperationException("Processor $javaClass can't able to process ${Message::javaClass}")
    }
    fun handle(intervalEventId: EventID, message: RawMessage) {
        throw UnsupportedOperationException("Processor $javaClass can't able to process ${RawMessage::javaClass}")
    }
    fun handle(intervalEventId: EventID, event: Event) {
        throw UnsupportedOperationException("Processor $javaClass can't able to process ${Event::javaClass}")
    }
    fun serializeState(): ByteArray? = null
}