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

package com.exactpro.th2.processor.core.event

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.bean.IRow
import com.exactpro.th2.common.event.bean.Table
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic.ScopeStat
import com.exactpro.th2.dataprovider.lw.grpc.EventQueueSearchRequest
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.Crawler
import com.exactpro.th2.processor.core.event.controller.EventController
import mu.KotlinLogging
import java.time.Instant

class EventCrawler(
    context: Context,
    processor: IProcessor,
) : Crawler<EventBatch>(
    context.commonFactory,
    context.commonFactory.eventBatchRouter,
    context.eventBatcher,
    processor,
    context.processorEventId,
    context.configuration,
) {
    private val bookToScopes: Map<String, Set<String>> = requireNotNull(
        requireNotNull(crawlerConfiguration.events) {
            "The `crawler.events` configuration can not be null"
        }.bookToScopes
    )

    private val bookScopes: List<EventQueueSearchRequest.BookScopes> = bookToScopes.map { (book, scopes) ->
        EventQueueSearchRequest.BookScopes.newBuilder().apply {
            bookIdBuilder.apply { name = book }
            scopes.forEach { scope ->
                addScopeBuilder().apply { name = scope }
            }
        }.build()
    }
    override fun process(from: Instant, to: Instant, intervalEventId: EventID) {
        val fromTimestamp = from.toTimestamp()
        val toTimestamp = to.toTimestamp()
        controller = EventController(processor, intervalEventId, fromTimestamp, toTimestamp, bookToScopes)

        val request = EventQueueSearchRequest.newBuilder().apply {
            startTimestamp = fromTimestamp
            endTimestamp = toTimestamp
            externalQueue = queue
            syncInterval = this@EventCrawler.syncInterval

            addAllEventScopes(bookScopes)
        }.build()

        K_LOGGER.info { "Request ${request.toJson()}" }
        dataProvider.searchEvents(request)
            .also { response ->
                reportResponse(response, intervalEventId)
                controller.expected(response)
                check(controller.await(awaitTimeout, awaitUnit)) {
                    "Quantification failure after ($awaitTimeout:$awaitUnit waiting, controller $controller)"
                }
            }
    }
    private fun reportResponse(response: EventLoadedStatistic, intervalEventId: EventID) {
        K_LOGGER.info { "Request ${response.toJson()}" }
        Event.start()
            .name("Requested events")
            .type(EVENT_TYPE_REQUEST_TO_DATA_PROVIDER)
            .bodyData(Table().apply {
                type = "Event statistic"
                fields = response.statList.map(::toRow)
            })
            .toProto(intervalEventId)
            .also(eventBatcher::onEvent)
    }
    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
        private data class StatisticRow(val book: String, val scope: String, val count: Long): IRow
        fun toRow(scopeStat: ScopeStat): IRow = StatisticRow(scopeStat.bookId.name, scopeStat.scope.name, scopeStat.count)
    }
}