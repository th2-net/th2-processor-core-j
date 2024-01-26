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

package com.exactpro.th2.processor.core.message

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.bean.IRow
import com.exactpro.th2.common.event.bean.Table
import com.exactpro.th2.processor.core.configuration.MessageKind.RAW_MESSAGE
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsQueueSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsQueueSearchRequest.BookGroups
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.Crawler
import com.exactpro.th2.processor.core.configuration.MessageKind
import mu.KotlinLogging
import java.time.Instant

internal abstract class AbstractMessageCrawler<T>(
    context: Context,
    processor: IProcessor,
    router: MessageRouter<T>
) : Crawler<T>(
    context.commonFactory,
    router,
    context.eventBatcher,
    processor,
    context.processorEventId,
    context.configuration
) {
    protected val messageKinds: Set<MessageKind> = requireNotNull(crawlerConfiguration.messages).messageKinds

    protected val bookToGroups: Map<String, Set<String>> = requireNotNull(
        requireNotNull(crawlerConfiguration.messages) {
            "The `crawler.messages` configuration can not be null"
        }.bookToGroups
    )

    private val bookGroups: List<BookGroups> = bookToGroups.map { (book, groups) ->
        BookGroups.newBuilder().apply {
            bookIdBuilder.apply { name = book }
            groups.forEach { group ->
                addGroupBuilder().apply { name = group }
            }
        }.build()
    }
    override fun process(from: Instant, to: Instant, intervalEventId: EventID) {
        controller = createController(intervalEventId, from, to)

        val request = MessageGroupsQueueSearchRequest.newBuilder().apply {
            startTimestamp = from.toTimestamp()
            endTimestamp = to.toTimestamp()
            externalQueue = queue
            syncInterval = this@AbstractMessageCrawler.syncInterval
            sendRawDirectly = messageKinds.contains(RAW_MESSAGE)
            rawOnly = messageKinds.size == 1 && messageKinds.contains(RAW_MESSAGE)

            addAllMessageGroup(bookGroups)
        }.build()

        K_LOGGER.info { "Request ${request.toJson()}" }
        dataProvider.searchMessageGroups(request)
            .also { response ->
                reportResponse(response, intervalEventId)
                controller.expected(response)
                check(controller.await(awaitTimeout, awaitUnit)) {
                    "Quantification failure after ($awaitTimeout:$awaitUnit waiting, controller $controller)"
                }
            }
    }

    protected abstract fun createController(intervalEventId: EventID, from: Instant, to: Instant): Controller<T>

    private fun reportResponse(response: MessageLoadedStatistic, intervalEventId: EventID) {
        K_LOGGER.info { "Request ${response.toJson()}" }
        Event.start()
            .name("Requested messages")
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
        private data class StatisticRow(val book: String, val group: String, val count: Long): IRow
        fun toRow(scopeStat: MessageLoadedStatistic.GroupStat): IRow = StatisticRow(scopeStat.bookId.name, scopeStat.group.name, scopeStat.count)
    }
}