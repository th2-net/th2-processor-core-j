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
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.event.book
import com.exactpro.th2.common.utils.event.logId
import com.exactpro.th2.common.utils.event.scope
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.EventScope
import com.exactpro.th2.processor.core.state.StateUpdater
import com.exactpro.th2.processor.utility.check
import com.exactpro.th2.processor.utility.compareTo
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

    fun plus(func: StateUpdater<Event>.() -> Unit): Boolean {
        val temporaryState = mutableMapOf<StateKey, Long>()
        object : StateUpdater<Event> {
            @Suppress("PARAMETER_NAME_CHANGED_ON_OVERRIDE")
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
                "Scope statistic has not got information about scope. ${scopeStat.toJson()}"
            }
            check(scope.name.isNotBlank()) {
                "Scope statistic has empty Scope name. ${scopeStat.toJson()}"
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

        K_LOGGER.debug { "Minus operation executed: ${eventLoadedStatistic.toJson()}, state = $bookAndScopeToNumber, need check = $needCheck" }
        return needCheck && isStateEmpty
    }

    private fun Event.toStateKey(): StateKey {
        val timestamp = id.startTimestamp
        check(timestamp >= startTime && timestamp < endTime) {
            "Out of interval event ${logId}, " +
                    "actual ${Timestamps.toString(timestamp)}, " +
                    "expected [${Timestamps.toString(startTime)} - ${Timestamps.toString(endTime)})"
        }

        val book = this.book.also { check(book.isNotBlank()) { "Event $logId hasn't got book" } }
        val scope = this.scope.also { check(scope.isNotBlank()) { "Event $logId hasn't got scope" } }
        bookToScopes.check(book, scope) {
            "Unexpected event ${logId}, book ${book}, scope $scope"
        }

        return StateKey(book, scope)
    }

    private fun EventLoadedStatistic.ScopeStat.toStateKey(): StateKey {
        check(hasBookId()) {
            "Scope statistic has not got information about book. ${this.toJson()}"
        }
        check(bookId.name.isNotBlank()) {
            "Scope statistic has empty book name. ${this.toJson()}"
        }

        check(hasScope()) {
            "Scope statistic has not got information about scope. ${this.toJson()}"
        }
        check(scope.name.isNotBlank()) {
            "Scope statistic has empty scope name. ${this.toJson()}"
        }

        bookToScopes.check(bookId.name, scope.name) {
            "Unexpected statistic for book ${bookId.name}, group ${scope.name}. ${this.toJson()}"
        }

        return StateKey(bookId.name, scope.name)
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        private data class StateKey(val book: String, val scope: String)
    }
}