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

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.utils.event.logId
import com.exactpro.th2.processor.Application
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.ProcessorException
import com.exactpro.th2.processor.core.configuration.RealtimeConfiguration
import com.exactpro.th2.processor.core.state.RealtimeState
import com.exactpro.th2.processor.utility.OBJECT_MAPPER
import mu.KotlinLogging

abstract class AbstractRealtimeStrategy(context: Context): AbstractStrategy(context) {

    private val realtimeConfig: RealtimeConfiguration = requireNotNull(context.configuration.realtime) {
        Application.CONFIGURATION_ERROR_PREFIX +
                "the `realtime` setting can be null"
    }

    private val eventMonitor: SubscriberMonitor
    private val messageMonitor: SubscriberMonitor
    protected val processor: IProcessor

    init {
        val state = recoverState(RealtimeState::class.java)
        processor = createProcessor(state?.processorState)

        messageMonitor = if (realtimeConfig.enableMessageSubscribtion) {
            subscribeToMessages(context)
        } else { EMPTY_MONITOR }

        eventMonitor = if (realtimeConfig.enableEventSubscribtion) {
            context.commonFactory.eventBatchRouter.subscribe({ _, batch ->
                val exceptionList = mutableListOf<Throwable>()
                batch.eventsList.forEach { event ->
                    runCatching {
                        processor.handle(context.processorEventId, event)
                    }.onFailure { e ->
                        exceptionList.add(e)
                        reportHandleEventError(event.id, e)
                    }
                }
                checkAndThrow(exceptionList)
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

    protected abstract fun subscribeToMessages(context: Context): SubscriberMonitor

    protected fun checkAndThrow(exceptionList: List<Throwable>) {
        if (exceptionList.isNotEmpty()) {
            throw ProcessorException("Handling failure").apply {
                exceptionList.forEach(this::addSuppressed)
            }
        }
    }
    protected fun reportHandleMessageError(messageId: MessageID, e: Throwable) {
        createEvent(e)
            .type(MESSAGE_PROCESSING_FAILURE_EVENT_TYPE)
            .messageID(messageId)
            .toProto(context.processorEventId)
            .also(context.eventBatcher::onEvent)
    }
    private fun reportHandleEventError(eventID: EventID, e: Throwable) {
        createEvent(e)
            .type(EVENT_PROCESSING_FAILURE_EVENT_TYPE)
            .bodyData(Reference(eventID.logId))
            .toProto(context.processorEventId)
            .also(context.eventBatcher::onEvent)
    }
    private fun createEvent(e: Throwable): Event = Event.start()
        .name("Handle data failure ${e.message}")
        .status(Event.Status.FAILED)
        .exception(e, true)
    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
        private const val EVENT_PROCESSING_FAILURE_EVENT_TYPE: String = "Event processing failure"
        private const val MESSAGE_PROCESSING_FAILURE_EVENT_TYPE: String = "Message processing failure"

        internal const val IN_ATTRIBUTE = "in"

        private val EMPTY_MONITOR = SubscriberMonitor { }

        private data class Reference(
            val type: String = "reference",
            val eventId: String? = null,
            val messageId: String? = null,
        ): IBodyData
    }
}