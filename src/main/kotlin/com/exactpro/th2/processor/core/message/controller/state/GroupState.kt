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

package com.exactpro.th2.processor.core.message.controller.state

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.message.logId
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.message.book
import com.exactpro.th2.common.utils.message.sessionGroup
import com.exactpro.th2.common.utils.message.timestamp
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic.GroupStat
import com.exactpro.th2.processor.core.state.StateUpdater
import com.exactpro.th2.processor.utility.compareTo
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

//TODO: Extract common part
internal class GroupState(
    private val startTime: Timestamp,
    private val endTime: Timestamp,
    private val kind: AnyMessage.KindCase,
    private val bookToGroups: Map<String, Set<String>>
) {
    private val groupToNumber = ConcurrentHashMap<StateKey, Long>()
    val isStateEmpty: Boolean
        get() = groupToNumber.isEmpty()

    fun plus(func: StateUpdater<AnyMessage>.() -> Unit): Boolean {
        val temporaryState = mutableMapOf<StateKey, Long>()
        object : StateUpdater<AnyMessage> {
            @Suppress("PARAMETER_NAME_CHANGED_ON_OVERRIDE")
            override fun updateState(anyMessage: AnyMessage) {
                temporaryState.compute(anyMessage.toStateKey()) { _, current -> (current ?: 0L) + 1 }
            }
        }.func()

        var needCheck = false
        temporaryState.forEach { (group, count) ->
            groupToNumber.compute(group) { _, previous ->
                when (val result = (previous ?: 0L) + count) {
                    0L -> {
                        needCheck = true
                        null
                    }

                    else -> result
                }
            }
        }

        K_LOGGER.debug { "Plus operation executed: delta = $temporaryState, state = $groupToNumber, need check = $needCheck" }
        return needCheck && isStateEmpty
    }

    fun minus(loadedStatistic: MessageLoadedStatistic): Boolean {
        var needCheck = false
        loadedStatistic.statList.forEach { groupStat ->

            groupToNumber.compute(groupStat.toStateKey()) { _, previous ->
                when (val result = (previous ?: 0L) - groupStat.count) {
                    0L -> {
                        needCheck = true
                        null
                    }
                    else -> result
                }
            }
        }

        K_LOGGER.debug { "Minus operation executed: ${loadedStatistic.toJson()}, state = $groupToNumber, need check = $needCheck" }
        return needCheck && isStateEmpty
    }

    private fun AnyMessage.toStateKey(): StateKey {
        check(kindCase == kind) {
            "Incorrect message kind ${logId}, " +
                    "actual ${kindCase}, expected $kind"
        }

        val timestamp = timestamp
        check(timestamp >= startTime && timestamp < endTime) {
            "Out of interval message ${logId}, " +
                    "actual ${Timestamps.toString(timestamp)}, " +
                    "expected [${Timestamps.toString(startTime)} - ${Timestamps.toString(endTime)})"
        }

        val book = requireNotNull(book) {
            "Group statistic has empty book name. ${this.toJson()}"
        }
        val group = requireNotNull(sessionGroup) {
            "Group statistic has empty group name. ${this.toJson()}"
        }

        check(bookToGroups[book]?.contains(group) ?: false) {
            "Unexpected message ${logId}, book $book, group $group"
        }

        return StateKey(book, group)
    }

    private fun GroupStat.toStateKey(): StateKey {
        check(hasBookId()) {
            "Group statistic has not got information about book. ${this.toJson()}"
        }
        check(bookId.name.isNotBlank()) {
            "Group statistic has empty book name. ${this.toJson()}"
        }

        check(hasGroup()) {
            "Group statistic has not got information about group. ${this.toJson()}"
        }
        check(group.name.isNotBlank()) {
            "Group statistic has empty group name. ${this.toJson()}"
        }

        check(bookToGroups[bookId.name]?.contains(group.name) ?: false) {
            "Unexpected statistic for book ${bookId.name}, group ${group.name}. ${this.toJson()}"
        }

        return StateKey(bookId.name, group.name)
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private data class StateKey(val book: String, val group: String)
    }
}