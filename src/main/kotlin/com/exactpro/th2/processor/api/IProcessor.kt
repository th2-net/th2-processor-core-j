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

package com.exactpro.th2.processor.api

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import javax.annotation.concurrent.ThreadSafe
import com.exactpro.th2.common.grpc.Message as ProtobufMessage
import com.exactpro.th2.common.grpc.RawMessage as ProtobufRawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage as TransportRawMessage

@ThreadSafe
interface IProcessor : AutoCloseable {
    fun handle(intervalEventId: EventID, message: ProtobufMessage) {
        throw UnsupportedOperationException("Processor $javaClass can't able to process ${ProtobufMessage::class.java.simpleName}")
    }
    fun handle(intervalEventId: EventID, message: ProtobufRawMessage) {
        throw UnsupportedOperationException("Processor $javaClass can't able to process ${ProtobufRawMessage::class.java.simpleName}")
    }
    fun handle(intervalEventId: EventID, message: ParsedMessage) {
        throw UnsupportedOperationException("Processor $javaClass can't able to process ${ParsedMessage::class.java.simpleName}")
    }
    fun handle(intervalEventId: EventID, message: TransportRawMessage) {
        throw UnsupportedOperationException("Processor $javaClass can't able to process ${TransportRawMessage::class.java.simpleName}")
    }
    fun handle(intervalEventId: EventID, event: Event) {
        throw UnsupportedOperationException("Processor $javaClass can't able to process ${Event::class.java.simpleName}")
    }
    fun serializeState(): ByteArray? = null
    override fun close() { }
}