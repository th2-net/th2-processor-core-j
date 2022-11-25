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

package com.exactpro.th2.processor.core.event

import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.lw.grpc.EventQueueSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Crawler
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.event.controller.EventController
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.Timestamp
import mu.KotlinLogging

class EventCrawler(
    eventRouter: MessageRouter<EventBatch>,
    private val dataProvider: QueueDataProviderService,
    configuration: Configuration,
    processor: IProcessor
) : Crawler<EventBatch>(
    eventRouter,
    configuration,
    processor,
) {
    private val bookToScopes = requireNotNull(
        requireNotNull(configuration.events).bookToScopes
    ).also {
        check(it.isNotEmpty()) {
            "Incorrect configuration parameters: the `bookToScopes` option is empty"
        }
    }

    private val bookScopes = bookToScopes.map { (book, scopes) ->
        EventQueueSearchRequest.BookScopes.newBuilder().apply {
            bookIdBuilder.apply { name = book }
            scopes.forEach { scope ->
                addScopeBuilder().apply { name = scope }
            }
        }.build()
    }
    override fun process(from: Timestamp, to: Timestamp) {
        controller = EventController(processor, from, to, bookToScopes)

        val request = EventQueueSearchRequest.newBuilder().apply {
            startTimestamp = from
            endTimestamp = to
            externalQueue = queue

            addAllEventScopes(bookScopes)
        }.build()

        K_LOGGER.info { "Request ${shortDebugString(request)}" }
        dataProvider.searchEvents(request)
            .also { response ->
                K_LOGGER.info { "Request ${shortDebugString(response)}" }
                controller.expected(response)
                check(controller.await(awaitTimeout, awaitUnit)) {
                    "Quantification failure after ($awaitTimeout:$awaitUnit waiting, controller $controller)"
                }
            }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}