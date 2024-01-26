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

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.exactpro.th2.processor.core.configuration.MessageKind.MESSAGE
import com.exactpro.th2.processor.core.configuration.MessageKind.RAW_MESSAGE
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argThat
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import java.time.Instant

abstract class AbstractGroupMessageCrawlerTest<T> {

    private val dataProvider = mock<QueueDataProviderService> {
        on { searchMessageGroups(any()) }.thenReturn(MessageLoadedStatistic.getDefaultInstance())
    }
    private val grpcRouter = mock<GrpcRouter> {
        on { getService(eq(QueueDataProviderService::class.java)) }.thenReturn(dataProvider)
    }
    private val eventBatcher = mock<EventBatcher> {  }

    protected val commonFactory = mock<CommonFactory> {
        on { grpcRouter }.thenReturn(grpcRouter)
    }
    protected val monitor = mock<ExclusiveSubscriberMonitor> {
        on { queue }.thenReturn(EXCLUSIVE_QUEUE)
    }
    protected val processor = mock<IProcessor> {  }
    protected val context = mock<Context> {
        on { commonFactory }.thenReturn(commonFactory)
        on { eventBatcher }.thenReturn(eventBatcher)
        on { processorEventId }.thenReturn(PROCESSOR_EVENT_ID)
    }

    @Test
    fun `test response for both kinds`() {
        val crawler = createCrawler(setOf(MESSAGE, RAW_MESSAGE))
        crawler.processInterval(FROM, TO, INTERVAL_EVENT_ID)

        verify(dataProvider, times(1)).searchMessageGroups(argThat { argument ->
            !argument.rawOnly && argument.sendRawDirectly
        })
    }

    @Test
    fun `test response for message kind`() {
        val crawler = createCrawler(setOf(MESSAGE))
        crawler.processInterval(FROM, TO, INTERVAL_EVENT_ID)

        verify(dataProvider, times(1)).searchMessageGroups(argThat { argument ->
            !argument.rawOnly && !argument.sendRawDirectly
        })
    }

    @Test
    fun `test response for raw message kind`() {
        val crawler = createCrawler(setOf(RAW_MESSAGE))
        crawler.processInterval(FROM, TO, INTERVAL_EVENT_ID)

        verify(dataProvider, times(1)).searchMessageGroups(argThat { argument ->
            argument.rawOnly && argument.sendRawDirectly
        })
    }

    internal abstract fun createCrawler(kinds: Set<MessageKind>): AbstractMessageCrawler<T>

    companion object {
        internal const val BOOK_NAME = "known-book"
        private const val SCOPE_NAME = "known-scope"
        private const val EXCLUSIVE_QUEUE = "exclusive-queue"

        internal val FROM = Instant.now()
        internal val TO = FROM.plusSeconds(1_000)
        private val PROCESSOR_EVENT_ID = EventID.newBuilder().apply {
            bookName = BOOK_NAME
            scope = SCOPE_NAME
        }.build()
        private val INTERVAL_EVENT_ID = PROCESSOR_EVENT_ID.toBuilder().build()
    }
}