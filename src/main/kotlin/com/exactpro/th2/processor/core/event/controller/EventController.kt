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

package com.exactpro.th2.processor.core.event.controller

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.event.controller.state.EventState
import com.exactpro.th2.processor.utility.ifTrue
import com.google.protobuf.Timestamp

internal class EventController(
    private val processor: IProcessor,
    intervalEventId: EventID,
    startTime: Timestamp,
    endTime: Timestamp,
    bookToScopes: Map<String, Set<String>>
) : Controller<EventBatch>(
    intervalEventId
) {
    private val eventState = EventState(startTime, endTime, bookToScopes)

    override val isStateComplete: Boolean
        get() = super.isStateComplete && eventState.isStateEmpty

    override fun actual(batch: EventBatch) {
        var needSignal  = false
        for (event in batch.eventsList) {
            val updateResult = updateState(event)
            needSignal = needSignal or updateResult

            updateLastProcessed(event.id.startTimestamp.toInstant())
            processor.handle(intervalEventId, event)
        }
        needSignal.ifTrue(::signal)
    }

    override fun expected(loadedStatistic: MessageLoadedStatistic): Unit = throw UnsupportedOperationException()

    override fun expected(loadedStatistic: EventLoadedStatistic) {
        eventState.minus(loadedStatistic).ifTrue(::signal)
        super.expected(loadedStatistic)
    }

    private fun updateState(event: Event): Boolean = eventState.plus(event)
}