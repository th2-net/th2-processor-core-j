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

package com.exactpro.th2.processor.core.event

import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.configuration.CrawlerConfiguration
import com.exactpro.th2.processor.core.configuration.EventConfiguration
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.time.Instant

class TestEventCrawler {

    private val dataProvider = mock<QueueDataProviderService> {
        on { searchEvents(any()) }.thenReturn(EventLoadedStatistic.getDefaultInstance())
    }
    private val grpcRouter = mock<GrpcRouter> {
        on { getService(eq(QueueDataProviderService::class.java)) }.thenReturn(dataProvider)
    }
    private val eventBatcher = mock<EventBatcher> {  }
    private val monitor = mock<ExclusiveSubscriberMonitor> {
        on { queue }.thenReturn(EXCLUSIVE_QUEUE)
    }
    private val eventRouter = mock<MessageRouter<EventBatch>> {
        on { subscribeExclusive(any()) }.thenReturn(monitor)
    }
    private val processor = mock<IProcessor> {  }
    private val crawlerConfiguration = spy(CrawlerConfiguration(
        events = EventConfiguration(mapOf(BOOK_NAME to setOf())),
        from = FROM.toString(),
        to = TO.toString(),
    ))
    private val configuration = Configuration(
        crawler = crawlerConfiguration,
        bookName = "test_book_name",
        processorSettings = mock {  }
    )
    private val commonFactory = mock<CommonFactory> {
        on { eventBatchRouter }.thenReturn(eventRouter)
        on { grpcRouter }.thenReturn(grpcRouter)
    }
    private val context = mock<Context> {
        on { commonFactory }.thenReturn(commonFactory)
        on { eventBatcher }.thenReturn(eventBatcher)
        on { configuration }.thenReturn(configuration)
        on { processorEventId }.thenReturn(PROCESSOR_EVENT_ID)
    }

    @Test
    fun `test response`() {
        val crawler = EventCrawler(context, processor)
        crawler.processInterval(FROM.toTimestamp(), TO.toTimestamp(), INTERVAL_EVENT_ID)

        verify(dataProvider, times(1)).searchEvents(argThat { argument ->
            argument.hasStartTimestamp()
                    && argument.hasEndTimestamp()
                    && argument.hasSyncInterval()
                    && argument.eventScopesCount > 0
        })
    }

    companion object {
        private const val BOOK_NAME = "known-book"
        private const val SCOPE_NAME = "known-scope"
        private const val EXCLUSIVE_QUEUE = "exclusive-queue"

        private val FROM = Instant.now()
        private val TO = FROM.plusSeconds(1_000)
        private val PROCESSOR_EVENT_ID = EventID.newBuilder().apply {
            bookName = BOOK_NAME
            scope = SCOPE_NAME
        }.build()
        private val INTERVAL_EVENT_ID = PROCESSOR_EVENT_ID.toBuilder().build()
    }
}