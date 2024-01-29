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
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.logId
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.message.book
import com.exactpro.th2.common.utils.message.sessionGroup
import com.exactpro.th2.common.utils.message.timestamp
import com.exactpro.th2.processor.core.message.AbstractMessageState
import com.exactpro.th2.processor.core.message.AbstractMessageState.Companion.StateKey
import com.exactpro.th2.processor.utility.check
import com.exactpro.th2.processor.utility.compareTo
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps

internal class CradleMessageGroupState(
    private val startTime: Timestamp,
    private val endTime: Timestamp,
    private val kinds: Set<AnyMessage.KindCase>,
    bookToGroups: Map<String, Set<String>>
): AbstractMessageState<MessageGroupBatch, MessageGroup>(
    kinds.size,
    bookToGroups
) {
    override fun MessageGroup.toStateKey(batch: MessageGroupBatch): StateKey {
        check()

        val firstMessage = requireNotNull(messagesList.firstOrNull()) {
            "Message group can't be empty"
        }

        val book = requireNotNull(firstMessage.book) {
            "Any message has empty book name. ${this.toJson()}"
        }
        val group = requireNotNull(firstMessage.sessionGroup) {
            "Any message has empty group name. ${this.toJson()}"
        }

        bookToGroups.check(book, group) {
            "Unexpected message ${firstMessage.logId}, book $book, group $group"
        }

        return StateKey(book, group)
    }

    private fun MessageGroup.check(): MessageGroup {
        val firstMessage = requireNotNull(messagesList.firstOrNull()) {
            "Message group can't be empty"
        }
        val timestamp = firstMessage.timestamp
        check(timestamp >= startTime && timestamp < endTime) {
            "Out of interval message ${firstMessage.logId}, " +
                    "actual ${Timestamps.toString(timestamp)}, " +
                    "expected [${Timestamps.toString(startTime)} - ${Timestamps.toString(endTime)})"
        }
        val kindSet = hashSetOf<AnyMessage.KindCase>()
        messagesList.forEach { anyMessage ->
            check(kinds.contains(anyMessage.kindCase)) {
                "Incorrect message kind ${anyMessage.logId}, " +
                        "actual ${anyMessage.kindCase}, expected one of $kinds"
            }
            kindSet.add(anyMessage.kindCase)
        }
        check(kindSet.size == 1) {
            "The ${firstMessage.logId} message group has messages with $kindSet kinds, expected only one kind"
        }
        return this
    }
}