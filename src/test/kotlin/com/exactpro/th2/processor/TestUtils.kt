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

package com.exactpro.th2.processor

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.processor.core.configuration.MessageKind
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import com.exactpro.th2.common.grpc.RawMessage as ProtobufRawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message as TramsportMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage as TransportRawMessage

const val STATE_SESSION_ALIAS = "state-session-alias"
const val SESSION_ALIAS = "known-session-alias"
const val KNOWN_BOOK = "known-book"
const val UNKNOWN_BOOK = "unknown-book"
const val KNOWN_GROUP = "known-group"
const val UNKNOWN_GROUP = "unknown-group"
const val KNOWN_SCOPE = "known-scope"
const val UNKNOWN_SCOPE = "unknown-scope"
const val MESSAGE_TYPE = "message-type"

val SEQUENCE_COUNTER = AtomicLong(Random.nextLong())

fun MessageGroup.Builder.message(
    kind: AnyMessage.KindCase,
    book: String,
    group: String,
    sessionAlias: String,
    timestamp: Instant,
    sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
) {
    when (kind) {
        AnyMessage.KindCase.MESSAGE -> this += protobufMessage(book, group, sessionAlias, timestamp, sequence)
        AnyMessage.KindCase.RAW_MESSAGE -> this += protobufRawMessage(book, group, sessionAlias, timestamp, sequence)
        else -> error("Unsupported kind $kind")
    }
}

fun protobufMessage(
    book: String,
    group: String,
    sessionAlias: String,
    timestamp: Instant,
    sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
): Message = Message.newBuilder().apply {
    metadataBuilder.apply {
        idBuilder.apply {
            bookName = book
            this.timestamp = timestamp.toTimestamp()
            this.sequence = sequence
            this.direction = SECOND
            connectionIdBuilder.apply {
                sessionGroup = group
                this.sessionAlias = sessionAlias
            }
        }
        messageType = MESSAGE_TYPE
    }
}.build()

fun protobufRawMessage(
    book: String,
    group: String,
    sessionAlias: String,
    timestamp: Instant,
    sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
): ProtobufRawMessage = ProtobufRawMessage.newBuilder().apply {
    metadataBuilder.apply {
        idBuilder.apply {
            bookName = book
            this.timestamp = timestamp.toTimestamp()
            this.sequence = sequence
            this.direction = SECOND
            connectionIdBuilder.apply {
                sessionGroup = group
                this.sessionAlias = sessionAlias
            }
        }
    }
}.build()

fun message(
    kind: MessageKind,
    sessionAlias: String,
    timestamp: Instant,
    sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
): TramsportMessage<*> = when (kind) {
    MessageKind.MESSAGE -> transportMessage(sessionAlias, timestamp, sequence)
    MessageKind.RAW_MESSAGE -> transportRawMessage(sessionAlias, timestamp, sequence)
    else -> error("Unsupported kind $kind")
}

fun transportMessage(
    sessionAlias: String,
    timestamp: Instant,
    sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
): ParsedMessage = ParsedMessage.builder().apply {
    idBuilder().apply {
        setTimestamp(timestamp)
        setSequence(sequence)
        setSessionAlias(sessionAlias)
        setDirection(Direction.OUTGOING)
    }
    setType(MESSAGE_TYPE)
    setBody(emptyMap())
}.build()

fun transportRawMessage(
    sessionAlias: String,
    timestamp: Instant,
    sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
): TransportRawMessage = TransportRawMessage.builder().apply {
    idBuilder().apply {
        setTimestamp(timestamp)
        setSequence(sequence)
        setSessionAlias(sessionAlias)
        setDirection(Direction.OUTGOING)
    }.build()
}.build()