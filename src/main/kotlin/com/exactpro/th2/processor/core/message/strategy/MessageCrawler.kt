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

package com.exactpro.th2.processor.core.message.strategy

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.message.controller.DummyController
import com.exactpro.th2.processor.core.controller.Controller
import com.google.protobuf.Timestamp

abstract class MessageCrawler (
    messageRouter: MessageRouter<MessageGroupBatch>,
    protected val dataProvider: QueueDataProviderService,
    protected val processor: IProcessor,
    configuration: Configuration,
) : AutoCloseable {

    private val monitor: ExclusiveSubscriberMonitor

    protected val queue: String
    protected val awaitTimeout = configuration.awaitTimeout
    protected val awaitUnit = configuration.awaitUnit
    protected val messageKind = requireNotNull(configuration.messages).messageKind

    @Volatile
    protected var controller: Controller = DummyController.INSTANT

    init {
        // FIXME: if connection is be broken, subscribtion doesn't recover (exclusive queue specific)
        monitor = messageRouter.subscribeExclusive { _, batch ->
            controller.actual(batch)
        }
        queue = monitor.queue
    }

    abstract fun processInterval(from: Timestamp, to: Timestamp)

    override fun close() {
        monitor.unsubscribe()
    }
}