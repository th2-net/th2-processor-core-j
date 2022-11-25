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

package com.exactpro.th2.processor.core.event.controller.state

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.EventScope
import com.exactpro.th2.processor.utility.scope
import com.google.protobuf.TextFormat.shortDebugString
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

//TODO: Extract common part
internal class EventState {
    private val bookAndScopeToNumber = ConcurrentHashMap<String, Long>()
    val isStateEmpty: Boolean
        get() = bookAndScopeToNumber.isEmpty()

    fun plus(func: StateUpdater.() -> Unit): Boolean {
        val temporaryState = mutableMapOf<String, Long>()
        object : StateUpdater {
            override fun update(event: Event) {
                temporaryState.compute(event.scope) { _, current -> (current ?: 0L) + 1 }
            }
        }.func()

        var needCheck = false
        temporaryState.forEach { (group, count) ->
            bookAndScopeToNumber.compute(group) { _, previous ->
                when (val result = (previous ?: 0L) + count) {
                    0L -> {
                        needCheck = true
                        null
                    }

                    else -> result
                }
            }
        }

        K_LOGGER.debug { "Plus operation executed: delta = $temporaryState, state = $bookAndScopeToNumber, need check = $needCheck" }
        return needCheck && isStateEmpty
    }

    fun minus(eventLoadedStatistic: EventLoadedStatistic): Boolean {
        var needCheck = false
        eventLoadedStatistic.statList.forEach { scopeStat ->
            val scope = scopeStat.scope
            check(scope !== EventScope.getDefaultInstance()) {
                "Scope statistic has not got information about scope. ${shortDebugString(scopeStat)}"
            }
            check(scope.name.isNotBlank()) {
                "Scope statistic has empty Scope name. ${shortDebugString(scopeStat)}"
            }

            bookAndScopeToNumber.compute(scope.name) { _, previous ->
                when (val result = (previous ?: 0L) - scopeStat.count) {
                    0L -> {
                        needCheck = true
                        null
                    }
                    else -> result
                }
            }
        }

        K_LOGGER.debug { "Minus operation executed: ${shortDebugString(eventLoadedStatistic)}, state = $bookAndScopeToNumber, need check = $needCheck" }
        return needCheck && isStateEmpty
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}