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
import com.exactpro.th2.processor.utility.book
import com.exactpro.th2.processor.utility.compare
import com.exactpro.th2.processor.utility.logId
import com.exactpro.th2.processor.utility.scope
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import mu.KotlinLogging
import java.util.concurrent.ConcurrentHashMap

//TODO: Extract common part
internal class EventState(
    private val startTime: Timestamp,
    private val endTime: Timestamp,
    private val bookToScopes: Map<String, Set<String>>
) {
    private val bookAndScopeToNumber = ConcurrentHashMap<StateKey, Long>()
    val isStateEmpty: Boolean
        get() = bookAndScopeToNumber.isEmpty()

    fun plus(func: StateUpdater.() -> Unit): Boolean {
        val temporaryState = mutableMapOf<StateKey, Long>()
        object : StateUpdater {
            override fun updateState(event: Event) {
                temporaryState.compute(event.toStateKey()) { _, current -> (current ?: 0L) + 1 }
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

            bookAndScopeToNumber.compute(scopeStat.toStateKey()) { _, previous ->
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

    private fun Event.toStateKey(): StateKey {
        val timestamp = id.startTimestamp
        check(timestamp.compare(startTime) >= 0 && timestamp.compare(endTime) < 0) {
            "Out of interval event ${logId}, " +
                    "actual ${Timestamps.toString(timestamp)}, " +
                    "expected [${Timestamps.toString(startTime)} - ${Timestamps.toString(endTime)})"
        }

        val book = this.book.also { check(book.isNotBlank()) { "Event $logId hasn't got book" } }
        val scope = this.scope.also { check(scope.isNotBlank()) { "Event $logId hasn't got scope" } }
        check(bookToScopes[book]?.contains(this.scope) ?: false) {
            "unexpected event ${logId}, book ${book}, scope $scope"
        }

        return StateKey(book, scope)
    }

    private fun EventLoadedStatistic.ScopeStat.toStateKey(): StateKey {
        check(hasBookId()) {
            "Scope statistic has not got information about book. ${shortDebugString(this)}"
        }
        check(bookId.name.isNotBlank()) {
            "Scope statistic has empty book name. ${shortDebugString(this)}"
        }

        check(hasScope()) {
            "Scope statistic has not got information about scope. ${shortDebugString(this)}"
        }
        check(scope.name.isNotBlank()) {
            "Scope statistic has empty scope name. ${shortDebugString(this)}"
        }

        check(bookToScopes[bookId.name]?.contains(scope.name) ?: false) {
            "Unexpected statistic for book ${bookId.name}, group ${scope.name}. ${shortDebugString(this)}"
        }

        return StateKey(bookId.name, scope.name)
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private data class StateKey(val book: String, val scope: String)
    }
}