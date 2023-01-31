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

package com.exactpro.th2.processor

import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.cradle.CradleConfiguration
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.EventLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageLoadedStatistic
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.api.ProcessorContext
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.configuration.CrawlerConfiguration
import com.exactpro.th2.processor.core.configuration.EventConfiguration
import com.exactpro.th2.processor.core.configuration.MessageConfiguration
import com.exactpro.th2.processor.core.configuration.MessageKind.MESSAGE
import com.exactpro.th2.processor.core.configuration.MessageKind.RAW_MESSAGE
import com.exactpro.th2.processor.core.configuration.RealtimeConfiguration
import com.exactpro.th2.processor.strategy.RealtimeStrategy.Companion.IN_ATTRIBUTE
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.auto.service.AutoService
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.spy
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant

class TestApplication {

    private val messageMonitor = mock<ExclusiveSubscriberMonitor> {
        on { queue }.thenReturn(MESSAGE_EXCLUSIVE_QUEUE)
    }
    private val messageListener = argumentCaptor<MessageListener<MessageGroupBatch>> { }
    private val messageRouter = mock<MessageRouter<MessageGroupBatch>> {
        on { subscribeExclusive(messageListener.capture()) }.thenReturn(messageMonitor)
        on { subscribe(messageListener.capture(), same(IN_ATTRIBUTE)) }.thenReturn(messageMonitor)
    }
    private val eventMonitor = mock<ExclusiveSubscriberMonitor> {
        on { queue }.thenReturn(EVENT_EXCLUSIVE_QUEUE)
    }
    private val eventListener = argumentCaptor<MessageListener<EventBatch>> { }
    private val eventRouter = mock<MessageRouter<EventBatch>> {
        on { subscribeExclusive(eventListener.capture()) }.thenReturn(eventMonitor)
        on { subscribe(eventListener.capture(), same(IN_ATTRIBUTE)) }.thenReturn(eventMonitor)
    }
    private val queueDataProvider = mock<QueueDataProviderService> { }
    private val dataProvider = mock<DataProviderService> {
        on { searchMessages(any()) }.thenReturn(setOf<MessageSearchResponse>().iterator())
    }
    private val grpcRouter = mock<GrpcRouter> {
        on { getService(eq(QueueDataProviderService::class.java)) }.thenReturn(queueDataProvider)
        on { getService(eq(DataProviderService::class.java)) }.thenReturn(dataProvider)
    }
    private val crawlerConfiguration = spy(CrawlerConfiguration(
        from = FROM.toString(),
        to = TO.toString(),
        intervalLength = INTERVAL_LENGTH,
        syncInterval = INTERVAL_LENGTH.dividedBy(2),
        awaitTimeout = 1,
    ))
    private val configuration = spy(Configuration(
        crawler = crawlerConfiguration,
        stateSessionAlias = STATE_SESSION_ALIAS,
        enableStoreState = true,
        processorSettings = mock { }
    ))
    private val cradleConfiguration = mock<CradleConfiguration> {
        on { cradleMaxMessageBatchSize }.thenReturn(1_024L * 1_024L)
    }
    private val commonFactory = mock<CommonFactory> {
        on { eventBatchRouter }.thenReturn(eventRouter)
        on { messageRouterMessageGroupBatch }.thenReturn(messageRouter)
        on { grpcRouter }.thenReturn(grpcRouter)
        on { getCustomConfiguration(eq(Configuration::class.java), any()) }.thenReturn(configuration)
        on { boxConfiguration }.thenReturn(BoxConfiguration().apply {
            boxName = "test-box"
            bookName = KNOWN_BOOK
        })
        on { cradleConfiguration }.thenReturn(cradleConfiguration)
        on { rootEventId }.thenReturn(ROOT_EVENT_ID)
    }

    @Test
    @Timeout(10)
    fun `test event, message, raw message crawling`() {
        mockEvents()
        mockMessages()
        Application(commonFactory).use(Application::run)
        verify()
        verifyCrawlerRouters()
    }

    @Test
    @Timeout(10)
    fun `test event crawling`() {
        mockEvents()
        Application(commonFactory).use(Application::run)
        verify()
        verifyCrawlerRouters()
    }

    @Test
    @Timeout(10)
    fun `test message, raw message crawling`() {
        mockMessages()
        Application(commonFactory).use(Application::run)
        verify()
        verifyCrawlerRouters()
    }

    @Test
    fun `test realtime mode`() {
        mockRealtime()
        Application(commonFactory).use(Application::run)
        verify()
        val eventSender = argumentCaptor<EventBatch> { }
        verify(eventRouter, times(2)).sendAll(eventSender.capture())
        verify(messageRouter, times(1)).sendAll(any())

        verify(eventRouter, times(1)).subscribe(any(), same(IN_ATTRIBUTE))
        verify(eventMonitor, times(1)).unsubscribe()

        verify(messageRouter, times(1)).subscribe(any(), same(IN_ATTRIBUTE))
        verify(messageMonitor, times(1)).unsubscribe()
    }

    private fun verify() {
        verify(dataProvider, times(1)).searchMessages(any())
    }

    private fun verifyCrawlerRouters() {
        verify(messageRouter, times(1)).sendAll(any())
        //TODO: processor, start processing, end processing, processing complete, ?, state loading
        verify(eventRouter, times(6)).sendAll(any())
    }

    private fun mockMessages() {
        whenever(crawlerConfiguration.messages)
            .thenReturn(MessageConfiguration(setOf(MESSAGE, RAW_MESSAGE), mapOf(KNOWN_BOOK to setOf())))
        whenever(queueDataProvider.searchMessageGroups(any())).thenAnswer {
            with(messageListener.firstValue) {
                handle(DELIVERY_METADATA, MessageGroupBatch.newBuilder().apply {
                    addGroupsBuilder().apply {
                        this += message(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(1), 1)
                        this += message(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(1), 1)
                    }
                    addGroupsBuilder().apply {
                        this += rawMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(1), 1)
                    }
                }.build())
                handle(DELIVERY_METADATA, MessageGroupBatch.newBuilder().apply {
                    addGroupsBuilder().apply {
                        this += message(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(2), 2)
                    }
                    addGroupsBuilder().apply {
                        this += message(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(3), 3)
                    }
                    addGroupsBuilder().apply {
                        this += rawMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(2), 2)
                    }
                    addGroupsBuilder().apply {
                        this += rawMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(3), 3)
                    }
                }.build())
            }
            MessageLoadedStatistic.newBuilder().apply {
                addStatBuilder().apply {
                    bookIdBuilder.apply { name = KNOWN_BOOK }
                    groupBuilder.apply { name = KNOWN_GROUP }
                    count = 3
                }
            }.build()
        }
    }

    private fun mockRealtime() {
        whenever(configuration.crawler)
            .thenReturn(null)
        whenever(configuration.realtime)
            .thenReturn(RealtimeConfiguration(enableMessageSubscribtion = true, enableEventSubscribtion = true))
    }
    private fun mockEvents() {
        whenever(crawlerConfiguration.events)
            .thenReturn(EventConfiguration(mapOf(KNOWN_BOOK to setOf())))
        whenever(queueDataProvider.searchEvents(any())).thenAnswer {
            with(eventListener.firstValue) {
                handle(DELIVERY_METADATA, EventBatch.newBuilder().apply {
                    addEvent("1")
                    addEvent("2")
                }.build())
                handle(DELIVERY_METADATA, EventBatch.newBuilder().apply {
                    addEvent("3")
                }.build())
            }

            EventLoadedStatistic.newBuilder().apply {
                addStatBuilder().apply {
                    bookIdBuilder.apply { name = KNOWN_BOOK }
                    scopeBuilder.apply { name = KNOWN_SCOPE }
                    count = 3
                }
            }.build()
        }
    }

    companion object {
        private const val MESSAGE_EXCLUSIVE_QUEUE = "message-exclusive-queue"
        private const val EVENT_EXCLUSIVE_QUEUE = "event-exclusive-queue"

        private val INTERVAL_LENGTH = Duration.ofMinutes(1)
        private val FROM = Instant.now()
        private val TO = FROM.plus(INTERVAL_LENGTH)
        private val ROOT_EVENT_ID = EventID.newBuilder().apply {
            bookName = KNOWN_BOOK
            scope = KNOWN_SCOPE
        }.build()

        private val DELIVERY_METADATA = DeliveryMetadata("test-tag", false)

        private fun EventBatch.Builder.addEvent(id: String) {
            addEventsBuilder().apply {
                idBuilder.apply {
                    bookName = KNOWN_BOOK
                    scope = KNOWN_SCOPE
                    startTimestamp = FROM.plusNanos(1).toTimestamp()
                    this.id = id
                }
            }
        }

        @Suppress("unused")
        @AutoService(IProcessorFactory::class)
        class TestFactory : IProcessorFactory {
            override fun registerModules(configureMapper: ObjectMapper) {}

            override fun create(context: ProcessorContext): IProcessor = TestProcessor()
        }

        class TestProcessor : IProcessor {
            override fun handle(intervalEventId: EventID, event: Event) {
                // do nothing
            }

            override fun handle(intervalEventId: EventID, message: RawMessage) {
                // do nothing
            }

            override fun handle(intervalEventId: EventID, message: Message) {
                // do nothing
            }
        }
    }

}