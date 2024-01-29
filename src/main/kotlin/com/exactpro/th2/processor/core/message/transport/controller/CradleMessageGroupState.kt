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

package com.exactpro.th2.processor.core.message.transport.controller

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.utils.message.transport.logId
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.exactpro.th2.processor.core.message.AbstractMessageState
import com.exactpro.th2.processor.core.message.AbstractMessageState.Companion.StateKey
import com.exactpro.th2.processor.utility.check
import com.exactpro.th2.processor.utility.protoKind
import java.time.Instant

internal class CradleMessageGroupState(
    private val startTime: Instant,
    private val endTime: Instant,
    private val kinds: Set<MessageKind>,
    bookToGroups: Map<String, Set<String>>
): AbstractMessageState<GroupBatch, MessageGroup>(
    kinds.size,
    bookToGroups
) {
    override fun MessageGroup.toStateKey(batch: GroupBatch): StateKey {
        check()

        val firstMessage = requireNotNull(messages.firstOrNull()) {
            "Message group can't be empty"
        }

        bookToGroups.check(batch.book, batch.sessionGroup) {
            "Unexpected message ${firstMessage.id}, book $batch, group ${batch.sessionGroup}"
        }

        return StateKey(batch.book, batch.sessionGroup)
    }

    private fun MessageGroup.check(): MessageGroup {
        val firstMessage = requireNotNull(messages.firstOrNull()) {
            "Message group can't be empty"
        }
        val timestamp = firstMessage.id.timestamp
        check(timestamp >= startTime && timestamp < endTime) {
            "Out of interval message ${firstMessage.id.logId}, " +
                    "actual $timestamp, " +
                    "expected [$startTime - $endTime)"
        }
        val kindSet = hashSetOf<MessageKind>()
        messages.forEach { message ->
            check(kinds.contains(message.protoKind)) {
                "Incorrect message kind ${message.id.logId}, " +
                        "actual ${message.javaClass}, expected one of $kinds"
            }
            kindSet.add(message.protoKind)
        }
        check(kindSet.size == 1) {
            "The ${firstMessage.id.logId} message group has messages with $kindSet kinds, expected only one kind"
        }
        return this
    }
}