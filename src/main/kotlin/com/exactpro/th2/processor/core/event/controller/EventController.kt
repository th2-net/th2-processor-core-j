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

package com.exactpro.th2.processor.core.event.controller

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.event.controller.state.EventState
import com.exactpro.th2.processor.core.event.controller.state.StateUpdater
import com.exactpro.th2.processor.utility.book
import com.exactpro.th2.processor.utility.compare
import com.exactpro.th2.processor.utility.ifFalse
import com.exactpro.th2.processor.utility.ifTrue
import com.exactpro.th2.processor.utility.logId
import com.exactpro.th2.processor.utility.scope
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps.toString
import mu.KotlinLogging

internal class EventController(
    private val processor: IProcessor,
    private val startTime: Timestamp,
    private val endTime: Timestamp,
    private val bookToScopes: Map<String, Set<String>>
) : Controller<EventBatch>() {
    private val eventState = EventState()

    override val isStateEmpty: Boolean
        get() = eventState.isStateEmpty

    override fun actual(batch: EventBatch) {
        updateState {
            for (event in batch.eventsList) {
                if (!eventCheck(event)) {
                    continue
                }

                // TODO: refactor looks strange
                updateLastProcessed(event.id.startTimestamp)
                update(event)
                processor.handle(event)
            }
        }.ifTrue(::signal)
    }

    override fun expected(loadedStatistic: EventLoadedStatistic) {
        eventState.minus(loadedStatistic).ifTrue(::signal)
    }

    private fun updateState(func: StateUpdater.() -> Unit): Boolean = eventState.plus(func)

    private fun eventCheck(event: Event): Boolean {
        val timestamp = event.id.startTimestamp
        return (timestamp.compare(startTime) >= 0 && timestamp.compare(endTime) < 0)
                    .ifFalse {
                        K_LOGGER.warn {
                            "Out of interval event ${event.logId}, " +
                                    "actual ${toString(timestamp)}, " +
                                    "expected [${toString(startTime)} - ${toString(endTime)})"
                        }
                    }
            && (bookToScopes[event.book]?.contains(event.scope) ?: false)
                    .ifFalse {
                        K_LOGGER.warn {
                            "unexpected event ${event.logId}, book ${event.book}, scope ${event.scope}"
                        }
                    }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}