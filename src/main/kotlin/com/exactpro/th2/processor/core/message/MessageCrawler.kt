/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.processor.core.message

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.grpc.CradleMessageGroupsRequest
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import com.exactpro.th2.dataprovider.grpc.Group
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.message.controller.DummyMessageController
import com.exactpro.th2.processor.core.message.controller.IMessageController
import com.exactpro.th2.processor.core.message.controller.MessageController
import com.google.protobuf.TextFormat.shortDebugString
import mu.KotlinLogging
import java.time.Instant

class MessageCrawler(
    messageRouter: MessageRouter<MessageGroupBatch>,
    private val dataProvider: DataProviderService,
    private val configuration: Configuration,
    private val processor: IProcessor,
) : AutoCloseable {
    private val groupSet = configuration.th2Groups.toSet()

    private val monitor: ExclusiveSubscriberMonitor
    @Volatile
    private var controller: IMessageController = DummyMessageController.INSTANT

    init {
        check(configuration.th2Groups.isNotEmpty()) {
            "Incorrect configuration parameters: the ${configuration.th2Groups} `th2 groups` option is empty"
        }

        monitor = messageRouter.subscribeExclusive { _, batch ->
            controller.actual(batch)
        }
    }

    /**
     * @return true if the current iteration process interval otherwise false
     */
    fun process(from: Instant, to: Instant): Boolean {
        controller = MessageController(
            processor,
            groupSet,
            from,
            to
        )

        val request = CradleMessageGroupsRequest.newBuilder().apply {
            startTimestamp = from.toTimestamp()
            endTimestamp = to.toTimestamp()
            externalUserQueue = monitor.queue

            val groupBuilder = Group.newBuilder()
            for (group in configuration.th2Groups) {
                groupBuilder.name = group
                addMessageGroup(groupBuilder.build())
            }
        }.build()

        K_LOGGER.info { "Request ${shortDebugString(request)}" }
        dataProvider.loadCradleMessageGroups(request)
            .also { response ->
                K_LOGGER.info { "Request ${shortDebugString(response)}" }
                controller.expected(response.messageIntervalInfo)
                check(controller.await(configuration.awaitTimeout, configuration.awaitUnit)) {
                    "Quantification failure after (${configuration.awaitTimeout}:${configuration.awaitUnit} waiting, controller $controller)"
                }
                controller = DummyMessageController.INSTANT
            }

        return true
    }

    override fun close() {
        monitor.unsubscribe()
    }

    fun serializeState(): ByteArray {
        return ByteArray(0)
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}