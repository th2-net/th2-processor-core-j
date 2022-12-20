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
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.message.sequence
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.message.direction
import com.exactpro.th2.common.utils.message.sessionAlias
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

typealias SessionAlias = String

class StreamState {
    private val lock = ReentrantReadWriteLock()
    private val actual: MutableMap<Direction, MutableMap<SessionAlias, MutableSet<Long>>> = EnumMap(Direction::class.java)
    private var expected: Long = 0

    fun actual(anyMessage: AnyMessage) = lock.write {
        with(anyMessage) {
            val sessionAlias = requireNotNull(sessionAlias) {
                "Any message has empty session alias. ${this.toJson()}"
            }

            actual.compute(direction) { _, sessionAliasToSequence ->
                sessionAliasToSequence
                    ?.apply {
                        compute(sessionAlias) { _, sequenceSet ->
                            sequenceSet
                                ?.apply { add(sequence) }
                                ?: hashSetOf(sequence)
                        }
                    }
                    ?: hashMapOf(sessionAlias to hashSetOf(sequence))

            }
        }
    }

    fun expected(expected: Long): Boolean = lock.write {
        check(expected > 0) {
            "The '$expected' expected value can't be negative"
        }
        this.expected += expected
        return@write this.expected == size()
    }

    /**
     * @return true if target value is reached
     */
    fun plus(streamState: StreamState): Boolean = lock.write {
        streamState.lock.read {
            streamState.actual.forEach { (direction, sessionAliasToSequence) ->
                actual.merge(direction, sessionAliasToSequence) { oldMap, newMap ->
                    oldMap.apply {
                        newMap.forEach { (sessionAlias, sequenceSet) ->
                            merge(sessionAlias, sequenceSet) { oldSet, newSet ->
                                oldSet.addAll(newSet)
                                if (oldSet.isEmpty()) null else oldSet
                            }
                        }
                    }
                    if (oldMap.isEmpty()) null else oldMap
                }
            }
            return@write this.expected == size()
        }
    }

    override fun toString(): String = lock.read {
        return "StreamState(map=$actual)"
    }

    private fun size(): Long = actual.values.asSequence()
            .flatMap(Map<SessionAlias, Set<Long>>::values)
            .map(Set<Long>::size)
            .map(Int::toLong)
            .sum()
}