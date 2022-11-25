/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.AnyMessageOrBuilder
import com.exactpro.th2.common.message.sessionAlias
import com.exactpro.th2.common.message.sessionGroup
import com.google.protobuf.Timestamp

//TODO: move to common-util
val AnyMessageOrBuilder.timestamp: Timestamp
    get() = when {
        hasMessage() -> message.metadata.id.timestamp
        hasRawMessage() -> rawMessage.metadata.id.timestamp
        else -> error("Unsupported message kind: $kindCase")
    }

val AnyMessage.book: String
    get() = when (kindCase) {
        AnyMessage.KindCase.MESSAGE -> message.metadata.id.bookName
        AnyMessage.KindCase.RAW_MESSAGE -> rawMessage.metadata.id.bookName
        else -> error("Unsupported message kind: $kindCase")
    }

val AnyMessageOrBuilder.group: String
    get() = when {
        hasMessage() -> message.sessionGroup.ifBlank { message.sessionAlias }
        hasRawMessage() -> rawMessage.sessionGroup.ifBlank { rawMessage.sessionAlias }
        else -> error("Unsupported message kind: $kindCase")
    }

fun Timestamp.compare(another: Timestamp): Int {
    val secDiff = seconds.compareTo(another.seconds)
    return if (secDiff != 0) secDiff else nanos.compareTo(another.nanos)
}