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

package com.exactpro.th2.processor.core.message.transport.controller

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.exactpro.th2.processor.core.message.transport.controller.state.CradleMessageGroupState
import com.exactpro.th2.processor.core.state.StateUpdater
import java.time.Instant

internal class CradleMessageGroupController(
    processor: IProcessor,
    intervalEventId: EventID,
    startTime: Instant,
    endTime: Instant,
    kinds: Set<MessageKind>,
    bookToGroups: Map<String, Set<String>>
) : MessageGroupController(
    processor,
    intervalEventId
) {
    private val cradleMessageGroupState = CradleMessageGroupState(startTime, endTime, kinds, bookToGroups)

    override val isStateComplete: Boolean
        get() = super.isStateComplete && cradleMessageGroupState.isStateEmpty

    override fun updateActualState(func: StateUpdater<GroupBatch, MessageGroup>.() -> Unit): Boolean = cradleMessageGroupState.plus(func)

    override fun updateExpectedState(loadedStatistic: MessageLoadedStatistic): Boolean = cradleMessageGroupState.minus(loadedStatistic)
}