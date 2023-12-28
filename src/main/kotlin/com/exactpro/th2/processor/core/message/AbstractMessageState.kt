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

package com.exactpro.th2.processor.core.message

import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic.GroupStat
import com.exactpro.th2.processor.utility.check
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

internal abstract class AbstractMessageState<B, G>(
    private val kinds: Int,
    protected val bookToGroups: Map<String, Set<String>>
) {
    private val groupToNumber = ConcurrentHashMap<StateKey, Long>()

    val isStateEmpty: Boolean
        get() = groupToNumber.isEmpty()

    fun plus(batch: B, group: G): Boolean {
        var needCheck = false
        val key = group.toStateKey(batch)
        groupToNumber.compute(key) { _, previous ->
            when (val result = (previous ?: 0L) + 1) {
                0L -> {
                    needCheck = true
                    null
                }
                else -> result
            }
        }

        K_LOGGER.debug { "Plus operation executed: state = $groupToNumber, need check = $needCheck" }
        return needCheck && isStateEmpty
    }

    fun minus(loadedStatistic: MessageLoadedStatistic): Boolean {
        var needCheck = false
        loadedStatistic.statList.forEach { groupStat ->
            groupToNumber.compute(groupStat.toStateKey()) { _, previous ->
                when (val result = (previous ?: 0L) - groupStat.count * kinds) {
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

    protected abstract fun G.toStateKey(batch: B): StateKey

    protected fun GroupStat.toStateKey(): StateKey {
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

        bookToGroups.check(bookId.name, group.name) {
            "Unexpected statistic for book ${bookId.name}, group ${group.name}. ${this.toJson()}"
        }

        return StateKey(bookId.name, group.name)
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        internal data class StateKey(val book: String, val group: String)
    }
}