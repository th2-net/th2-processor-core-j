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

package com.exactpro.th2.processor.core.message.controller

import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.message.controller.state.GroupState
import com.exactpro.th2.processor.core.state.StateUpdater
import com.google.protobuf.Timestamp

internal class GroupController(
    processor: IProcessor,
    intervalEventId: EventID,
    startTime: Timestamp,
    endTime: Timestamp,
    kind: AnyMessage.KindCase,
    bookToGroups: Map<String, Set<String>>
) : MessageController(
    processor,
    intervalEventId,
    kind
) {
    private val groupState = GroupState(startTime, endTime, kind, bookToGroups)

    override val isStateComplete: Boolean
        get() = super.isStateComplete && groupState.isStateEmpty

    override fun updateActualState(func: StateUpdater<AnyMessage>.() -> Unit): Boolean = groupState.plus(func)

    override fun updateExpectedState(loadedStatistic: MessageLoadedStatistic): Boolean = groupState.minus(loadedStatistic)
}