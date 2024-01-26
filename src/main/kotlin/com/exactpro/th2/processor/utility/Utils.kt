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

package com.exactpro.th2.processor.utility

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatchOrBuilder
import com.exactpro.th2.common.grpc.EventOrBuilder
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage
import com.exactpro.th2.common.utils.message.logId
import com.exactpro.th2.processor.core.HandleMessageException
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import mu.KLogger

val OBJECT_MAPPER: ObjectMapper = CBORMapper()
    .registerModule(JavaTimeModule())

val Message<*>.protoKind: MessageKind
    get() = when (this) {
        is ParsedMessage -> MessageKind.MESSAGE
        is RawMessage -> MessageKind.RAW_MESSAGE
        else -> error("Unsupported type ${this::class.java}")
    }

inline fun Boolean.ifTrue(func: () -> Unit): Boolean = this.also { if(it) func() }

//TODO: move to common-util
operator fun Timestamp.compareTo(another: Timestamp): Int = Timestamps.compare(this, another)

fun <T : EventOrBuilder> T.log(kLogger: KLogger, asInfo: Boolean = true): T {
    val func: () -> String = logFunc()
    when(status) {
        EventStatus.SUCCESS -> if (asInfo) kLogger.info(func) else kLogger.debug(func)
        EventStatus.FAILED -> kLogger.warn(func)
        else -> kLogger.error(func)
    }
    return this
}

fun <T : EventOrBuilder> T.logWarn(kLogger: KLogger): T {
    kLogger.warn(logFunc())
    return this
}

fun <T : EventBatchOrBuilder> T.log(kLogger: KLogger, asInfo: Boolean = true): T {
    eventsList.forEach { event -> event.log(kLogger, asInfo)}
    return this
}

fun <K, V> Map<K, Set<V>>.check(key: K, value: V, message: () -> String) {
    get(key).also { set ->
        check(set != null && (set.isEmpty() || set.contains(value)), message)
    }
}

fun Event.supplement(e: Exception): Event {
    if (e is HandleMessageException) {
        e.messageIds.forEach(this::messageID)
    }
    return this
}

private fun <T : EventOrBuilder> T.logFunc(): () -> String = {
    "Published event: name $name, type $type, status $status, ${
        if (attachedMessageIdsCount == 0) {
            ""
        } else {
            "messages ${attachedMessageIdsList.joinToString(prefix = "[", postfix = "]") { it.logId }}, "
        }
    }body ${body.toStringUtf8()}"
}
