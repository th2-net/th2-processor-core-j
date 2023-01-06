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
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.utils.message.toTimestamp
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

const val STATE_SESSION_ALIAS = "state-session-alias"
const val SESSION_ALIAS = "known-session-alias"
const val KNOWN_BOOK = "known-book"
const val UNKNOWN_BOOK = "unknown-book"
const val KNOWN_GROUP = "known-group"
const val UNKNOWN_GROUP = "unknown-group"
const val KNOWN_SCOPE = "known-scope"
const val UNKNOWN_SCOPE = "unknown-scope"

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
        AnyMessage.KindCase.MESSAGE -> this += message(book, group, sessionAlias, timestamp, sequence)
        AnyMessage.KindCase.RAW_MESSAGE -> this += rawMessage(book, group, sessionAlias, timestamp, sequence)
        else -> error("Unsupported kind $kind")
    }
}

fun message(
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
            connectionIdBuilder.apply {
                sessionGroup = group
                this.sessionAlias = sessionAlias
            }
        }
    }
}.build()

fun rawMessage(
    book: String,
    group: String,
    sessionAlias: String,
    timestamp: Instant,
    sequence: Long = SEQUENCE_COUNTER.incrementAndGet()
): RawMessage = RawMessage.newBuilder().apply {
    metadataBuilder.apply {
        idBuilder.apply {
            bookName = book
            this.timestamp = timestamp.toTimestamp()
            this.sequence = sequence
            connectionIdBuilder.apply {
                sessionGroup = group
                this.sessionAlias = sessionAlias
            }
        }
    }
}.build()