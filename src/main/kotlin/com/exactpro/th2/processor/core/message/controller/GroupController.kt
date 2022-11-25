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
import com.exactpro.th2.common.message.logId
import com.exactpro.th2.dataprovider.lw.grpc.LoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.message.controller.state.GroupState
import com.exactpro.th2.processor.core.message.controller.state.StateUpdater
import com.exactpro.th2.processor.utility.book
import com.exactpro.th2.processor.utility.group
import com.exactpro.th2.processor.utility.ifFalse
import com.exactpro.th2.processor.utility.ifTrue
import com.google.protobuf.Timestamp
import mu.KotlinLogging

internal class GroupController(
    processor: IProcessor,
    startTime: Timestamp,
    endTime: Timestamp,
    kind: AnyMessage.KindCase,
    private val bookToGroups: Map<String, Set<String>>
) : MessageController(
    processor,
    startTime,
    endTime,
    kind,
) {
    private val groupState = GroupState()

    override val isStateEmpty: Boolean
        get() = groupState.isStateEmpty

    override fun updateState(func: StateUpdater.() -> Unit): Boolean = groupState.plus(func)

    override fun expected(loadedStatistic: LoadedStatistic) {
        groupState.minus(loadedStatistic).ifTrue(::signal)
    }

    override fun messageCheck(anyMessage: AnyMessage): Boolean = super.messageCheck(anyMessage)
            && (bookToGroups[anyMessage.book]?.contains(anyMessage.group) ?: false)
                    .ifFalse {
                        K_LOGGER.warn {
                            "unexpected message ${anyMessage.logId}, book ${anyMessage.book}, group ${anyMessage.group}"
                        }
                    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}