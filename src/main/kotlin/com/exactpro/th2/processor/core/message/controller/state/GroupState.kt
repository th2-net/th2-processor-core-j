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
import com.exactpro.th2.dataprovider.lw.grpc.LoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.processor.utility.group
import com.google.protobuf.TextFormat.shortDebugString
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

internal class GroupState {
    private val groupToNumber = ConcurrentHashMap<String, Long>()

    val isStateEmpty: Boolean
        get() = groupToNumber.isEmpty()

    fun plus(func: StateUpdater.() -> Unit): Boolean {
        val temporaryState = mutableMapOf<String, Long>()
        object : StateUpdater {
            override fun update(anyMessage: AnyMessage) {
                val group: String = anyMessage.group
                check(group.isNotBlank()) {
                    "Message group is blank ${anyMessage.logId}"
                }

                temporaryState.compute(group) { _, current -> (current ?: 0L) + 1 }
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

    fun minus(loadedStatistic: LoadedStatistic): Boolean {
        var needCheck = false
        loadedStatistic.statList.forEach { groupStat ->
            val group = groupStat.group
            check(group !== MessageGroupsSearchRequest.Group.getDefaultInstance()) {
                "Group statistic has not got information about group. ${shortDebugString(groupStat)}"
            }
            check(group.name.isNotBlank()) {
                "Group statistic has empty group name. ${shortDebugString(groupStat)}"
            }

            groupToNumber.compute(group.name) { _, previous ->
                when (val result = (previous ?: 0L) - groupStat.count) {
                    0L -> {
                        needCheck = true
                        null
                    }
                    else -> result
                }
            }
        }

        K_LOGGER.debug { "Minus operation executed: ${shortDebugString(loadedStatistic)}, state = $groupToNumber, need check = $needCheck" }
        return needCheck && isStateEmpty
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}