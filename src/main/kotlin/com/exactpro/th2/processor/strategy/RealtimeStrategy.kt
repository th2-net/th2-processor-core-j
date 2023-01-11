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

package com.exactpro.th2.processor.strategy

import com.exactpro.th2.common.grpc.AnyMessage.KindCase.MESSAGE
import com.exactpro.th2.common.grpc.AnyMessage.KindCase.RAW_MESSAGE
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.processor.Application
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.configuration.RealtimeConfiguration
import com.exactpro.th2.processor.core.state.RealtimeState
import com.exactpro.th2.processor.utility.OBJECT_MAPPER
import mu.KotlinLogging

class RealtimeStrategy(context: Context): AbstractStrategy(context) {

    private val realtimeConfig: RealtimeConfiguration = requireNotNull(context.configuration.realtime) {
        Application.CONFIGURATION_ERROR_PREFIX +
                "the `realtime` setting can be null"
    }

    private val eventMonitor: SubscriberMonitor
    private val messageMonitor: SubscriberMonitor
    private val processor: IProcessor

    init {
        val state = recoverState(RealtimeState::class.java)
        processor = createProcessor(state?.processorState)

        messageMonitor = if (realtimeConfig.enableMessageSubscribtion) {
            context.commonFactory.messageRouterMessageGroupBatch.subscribe({ _, batch ->
                batch.groupsList.forEach { messageGroup ->
                    messageGroup.messagesList.forEach { anyMessage ->
                        when (anyMessage.kindCase) {
                            MESSAGE -> processor.handle(context.processorEventId, anyMessage.message)
                            RAW_MESSAGE -> processor.handle(context.processorEventId, anyMessage.rawMessage)
                            else -> error("Unsupported message kind: ${anyMessage.kindCase}")
                        }
                    }
                }
            }, IN_ATTRIBUTE)
        } else { EMPTY_MONITOR }

        eventMonitor = if (realtimeConfig.enableEventSubscribtion) {
            context.commonFactory.eventBatchRouter.subscribe({ _, batch ->
                batch.eventsList.forEach { event ->
                    processor.handle(context.processorEventId, event)
                }
            }, IN_ATTRIBUTE)
        } else { EMPTY_MONITOR }
        readiness.enable()
    }

    override fun close() {
        super.close()
        runCatching(messageMonitor::unsubscribe).onFailure { e ->
            K_LOGGER.error(e) { "Unsubscribing from messages failure" }
        }
        runCatching(eventMonitor::unsubscribe).onFailure { e ->
            K_LOGGER.error(e) { "Unsubscribing from events failure" }
        }
        if (context.configuration.enableStoreState) {
            OBJECT_MAPPER.writeValueAsBytes(RealtimeState(processor.serializeState()))
                .also { rawData -> context.stateStorage.saveState(context.processorEventId, rawData) }
        }
        runCatching(processor::close).onFailure { e ->
            K_LOGGER.error(e) { "Closing ${processor::class.java.simpleName} failure" }
        }
    }
    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        internal const val IN_ATTRIBUTE = "in"

        private val EMPTY_MONITOR = SubscriberMonitor { }
    }
}