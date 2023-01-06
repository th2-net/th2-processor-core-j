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

package com.exactpro.th2.processor.core

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.common.utils.message.toProtoDuration
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.configuration.Configuration
import com.google.protobuf.Message
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps.toString
import mu.KotlinLogging
import java.time.Duration

typealias ProtoDuration = com.google.protobuf.Duration

abstract class Crawler<T : Message>(
    protected val eventBatcher: EventBatcher,
    processorEventID: EventID,
    messageRouter: MessageRouter<T>,
    configuration: Configuration,
    protected val processor: IProcessor
) : AutoCloseable {

    private val monitor: ExclusiveSubscriberMonitor
    private val dummyController: Controller<T> = DummyController(processorEventID)

    protected val syncInterval: ProtoDuration = Duration.parse(configuration.syncInterval).toProtoDuration()
    protected val queue: String
    protected val awaitTimeout = configuration.awaitTimeout

    protected val awaitUnit = configuration.awaitUnit
    @Volatile
    protected var controller: Controller<T> = dummyController

    init {
        // FIXME: if connection is be broken, subscribtion doesn't recover (exclusive queue specific)
        monitor = messageRouter.subscribeExclusive { _, batch ->
            try {
                controller.actual(batch)
            } catch (e: Exception) {
                reportHandleError(controller.intervalEventId, e)
                throw e
            }
        }
        queue = monitor.queue
    }

    fun processInterval(from: Timestamp, to: Timestamp, intervalEventId: EventID) {
        try {
            process(from, to, intervalEventId)
        } catch (e: Exception) {
            reportProcessError(intervalEventId, from, to, e)
            throw e
        } finally {
            controller = dummyController
        }
    }

    protected open fun Event.supplement(e: Exception): Event = this
    private fun reportHandleError(intervalEventId: EventID, e: Exception) {
        K_LOGGER.error(e) { "Handle data failure" }
        eventBatcher.onEvent(
            Event.start()
                .name("Handle data failure ${e.message}")
                .type(EVENT_TYPE_PROCESS_INTERVAL)
                .status(FAILED)
                .exception(e, true)
                .supplement(e)
                .toProto(intervalEventId)
                .also(eventBatcher::onEvent)
        )
    }
    private fun reportProcessError(intervalEventId: EventID, from: Timestamp, to: Timestamp, e: Exception) {
        K_LOGGER.error(e) { "Process interval failure [${toString(from)} - ${toString(to)})" }
        eventBatcher.onEvent(
            Event.start()
                .name("Process interval failure ${e.message}")
                .type(EVENT_TYPE_PROCESS_INTERVAL)
                .status(FAILED)
                .exception(e, true)
                .supplement(e)
                .toProto(intervalEventId)
                .also(eventBatcher::onEvent)
        )
    }

    override fun close() {
        monitor.unsubscribe()
    }

    protected abstract fun process(from: Timestamp, to: Timestamp, intervalEventId: EventID)

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        @JvmStatic
        protected val EVENT_TYPE_REQUEST_TO_DATA_PROVIDER: String = "Request to data-provider"

        private const val EVENT_TYPE_PROCESS_INTERVAL: String = "Process interval"
    }
}