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

import com.exactpro.cradle.CradleEntitiesFactory
import com.exactpro.cradle.CradleManager
import com.exactpro.cradle.CradleStorage
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.message.plusAssign
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.box.configuration.BoxConfiguration
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.schema.grpc.router.GrpcRouter
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.toGroup
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
import com.exactpro.th2.processor.strategy.AbstractRealtimeStrategy.Companion.IN_ATTRIBUTE
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.auto.service.AutoService
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.same
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import strikt.api.expectThat
import strikt.assertions.contains
import strikt.assertions.isEqualTo
import strikt.assertions.single
import strikt.assertions.withElementAt
import java.time.Duration
import java.time.Instant
import com.exactpro.th2.common.grpc.RawMessage as ProtobufRawMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup.Companion as TransportMessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage as TransportRawMessage

class TestApplication {

    private val messageMonitor = mock<ExclusiveSubscriberMonitor> {
        on { queue }.thenReturn(MESSAGE_EXCLUSIVE_QUEUE)
    }
    private val protobufMessageListener = argumentCaptor<MessageListener<MessageGroupBatch>> { }
    private val protobufMessageRouter = mock<MessageRouter<MessageGroupBatch>> {
        on { subscribeExclusive(protobufMessageListener.capture()) }.thenReturn(messageMonitor)
        on { subscribe(protobufMessageListener.capture(), same(IN_ATTRIBUTE)) }.thenReturn(messageMonitor)
    }
    private val transportMessageListener = argumentCaptor<MessageListener<GroupBatch>> { }
    private val transportMessageRouter = mock<MessageRouter<GroupBatch>> {
        on { subscribeExclusive(transportMessageListener.capture()) }.thenReturn(messageMonitor)
        on { subscribe(transportMessageListener.capture(), same(IN_ATTRIBUTE)) }.thenReturn(messageMonitor)
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
    private val cradleManager = mock<CradleManager> {
        val cradleStorage = mock<CradleStorage> {
            on { entitiesFactory } doReturn CradleEntitiesFactory(
                1_024 * 1_024,
                1_024 * 1_024,
                100,
            )
        }
        on { storage } doReturn cradleStorage
    }
    private val commonFactory = mock<CommonFactory> {
        on { eventBatchRouter }.thenReturn(eventRouter)
        on { messageRouterMessageGroupBatch }.thenReturn(protobufMessageRouter)
        on { transportGroupBatchRouter }.thenReturn(transportMessageRouter)
        on { grpcRouter }.thenReturn(grpcRouter)
        on { boxConfiguration }.thenReturn(BoxConfiguration().apply {
            boxName = "test-box"
            bookName = KNOWN_BOOK
        })
        on { cradleManager }.thenReturn(cradleManager)
        on { rootEventId }.thenReturn(ROOT_EVENT_ID)
    }

    interface AbstractTest {
        fun `test realtime mode`()
        fun `test message, raw message crawling`()
        fun `test event crawling`()
        fun `test event, message, raw message crawling`()
    }

    @Nested
    inner class TransportTest : AbstractTest {
        @Test
        @Timeout(10)
        override fun `test event, message, raw message crawling`() {
            mockEvents()
            mockMessages()
            whenever(commonFactory.getCustomConfiguration(eq(Configuration::class.java), any()))
                .thenReturn(crawlerConfiguration(messages = true, events = true, useTransport = true))
            Application(commonFactory).use(Application::run)
            verify()
            verifyCrawlerRouters()
        }

        @Test
        @Timeout(10)
        override fun `test event crawling`() {
            mockEvents()
            whenever(commonFactory.getCustomConfiguration(eq(Configuration::class.java), any()))
                .thenReturn(crawlerConfiguration(messages = false, events = true, useTransport = true))
            Application(commonFactory).use(Application::run)
            verify()
            verifyCrawlerRouters()
        }

        @Test
        @Timeout(10)
        override fun `test message, raw message crawling`() {
            mockMessages()
            whenever(commonFactory.getCustomConfiguration(eq(Configuration::class.java), any()))
                .thenReturn(crawlerConfiguration(messages = true, events = false, useTransport = true))
            Application(commonFactory).use(Application::run)
            verify()
            verifyCrawlerRouters()
        }

        @Test
        override fun `test realtime mode`() {
            whenever(commonFactory.getCustomConfiguration(eq(Configuration::class.java), any()))
                .thenReturn(realtimeConfiguration(useTransport = true))
            Application(commonFactory).use(Application::run)
            verify()
            val eventSender = argumentCaptor<EventBatch> { }
            verify(eventRouter, times(2)).sendAll(eventSender.capture())
            verify(transportMessageRouter, times(1)).sendAll(any())

            verify(eventRouter, times(1)).subscribe(any(), same(IN_ATTRIBUTE))
            verify(eventMonitor, times(1)).unsubscribe()

            verify(transportMessageRouter, times(1)).subscribe(any(), same(IN_ATTRIBUTE))
            verify(messageMonitor, times(1)).unsubscribe()
        }

        private fun verifyCrawlerRouters() {
            verify(transportMessageRouter, times(1)).sendAll(any())
            verifyEventRouter()
        }

        private fun mockMessages() {
            whenever(queueDataProvider.searchMessageGroups(any())).thenAnswer {
                with(transportMessageListener.firstValue) {
                    handle(DELIVERY_METADATA, GroupBatch.builder().apply {
                        setBook(KNOWN_BOOK)
                        setSessionGroup(KNOWN_GROUP)
                        groupsBuilder().apply {
                            add(TransportMessageGroup.builder().apply {
                                messagesBuilder().apply {
                                    add(transportMessage(SESSION_ALIAS, FROM.plusNanos(1), 1))
                                    add(transportMessage(SESSION_ALIAS, FROM.plusNanos(1), 1))
                                }
                            }.build())
                            add(transportMessage(SESSION_ALIAS, FROM.plusNanos(1), 1).toGroup())
                        }
                    }.build())
                    handle(DELIVERY_METADATA, GroupBatch.builder().apply {
                        setBook(KNOWN_BOOK)
                        setSessionGroup(KNOWN_GROUP)
                        groupsBuilder().apply {
                            add(transportMessage(SESSION_ALIAS, FROM.plusNanos(2), 2).toGroup())
                            add(transportMessage(SESSION_ALIAS, FROM.plusNanos(3), 3).toGroup())
                            add(transportRawMessage(SESSION_ALIAS, FROM.plusNanos(2), 2).toGroup())
                            add(transportRawMessage(SESSION_ALIAS, FROM.plusNanos(3), 3).toGroup())
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
    }

    @Nested
    inner class ProtobufTest : AbstractTest {
        @Test
        @Timeout(10)
        override fun `test event, message, raw message crawling`() {
            mockEvents()
            mockMessages()
            whenever(commonFactory.getCustomConfiguration(eq(Configuration::class.java), any()))
                .thenReturn(crawlerConfiguration(messages = true, events = true, useTransport = false))
            Application(commonFactory).use(Application::run)
            verify()
            verifyCrawlerRouters()
        }

        @Test
        @Timeout(10)
        override fun `test event crawling`() {
            mockEvents()
            whenever(commonFactory.getCustomConfiguration(eq(Configuration::class.java), any()))
                .thenReturn(crawlerConfiguration(messages = false, events = true, useTransport = false))
            Application(commonFactory).use(Application::run)
            verify()
            verifyCrawlerRouters()
        }

        @Test
        @Timeout(10)
        override fun `test message, raw message crawling`() {
            mockMessages()
            whenever(commonFactory.getCustomConfiguration(eq(Configuration::class.java), any()))
                .thenReturn(crawlerConfiguration(messages = true, events = false, useTransport = false))
            Application(commonFactory).use(Application::run)
            verify()
            verifyCrawlerRouters()
        }

        @Test
        override fun `test realtime mode`() {
            whenever(commonFactory.getCustomConfiguration(eq(Configuration::class.java), any()))
                .thenReturn(realtimeConfiguration(useTransport = false))
            Application(commonFactory).use(Application::run)
            verify()
            val eventSender = argumentCaptor<EventBatch> { }
            verify(eventRouter, times(2)).sendAll(eventSender.capture())
            verify(protobufMessageRouter, times(1)).sendAll(any())

            verify(eventRouter, times(1)).subscribe(any(), same(IN_ATTRIBUTE))
            verify(eventMonitor, times(1)).unsubscribe()

            verify(protobufMessageRouter, times(1)).subscribe(any(), same(IN_ATTRIBUTE))
            verify(messageMonitor, times(1)).unsubscribe()
        }

        private fun verifyCrawlerRouters() {
            verify(protobufMessageRouter, times(1)).sendAll(any())
            verifyEventRouter()
        }

        private fun mockMessages() {
            whenever(queueDataProvider.searchMessageGroups(any())).thenAnswer {
                with(protobufMessageListener.firstValue) {
                    handle(DELIVERY_METADATA, MessageGroupBatch.newBuilder().apply {
                        addGroupsBuilder().apply {
                            this += protobufMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(1), 1)
                            this += protobufMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(1), 1)
                        }
                        addGroupsBuilder().apply {
                            this += protobufRawMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(1), 1)
                        }
                    }.build())
                    handle(DELIVERY_METADATA, MessageGroupBatch.newBuilder().apply {
                        addGroupsBuilder().apply {
                            this += protobufMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(2), 2)
                        }
                        addGroupsBuilder().apply {
                            this += protobufMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(3), 3)
                        }
                        addGroupsBuilder().apply {
                            this += protobufRawMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(2), 2)
                        }
                        addGroupsBuilder().apply {
                            this += protobufRawMessage(KNOWN_BOOK, KNOWN_GROUP, SESSION_ALIAS, FROM.plusNanos(3), 3)
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
    }

    private fun verifyEventRouter() {
        val captor = argumentCaptor<EventBatch> { }
        verify(eventRouter, times(6).description("Publish events")).sendAll(captor.capture())
        val processorEventId = captor.allValues.getOrNull(0)?.eventsList?.getOrNull(0)?.id
        val intervalEventId = captor.allValues.getOrNull(1)?.eventsList?.getOrNull(0)?.id
        expectThat(captor.allValues) {
            withElementAt(0) {
                get { eventsList }.single().and {
                    get { id }.apply {
                        get { bookName }.isEqualTo(ROOT_EVENT_ID.bookName)
                        get { scope }.isEqualTo(ROOT_EVENT_ID.scope)
                    }
                    get { parentId }.isEqualTo(ROOT_EVENT_ID)
                    get { name }.contains(Regex("Processor started $ISO_DATE_TIME_REGEX"))
                    get { type }.isEqualTo("Processor start")
                    get { body.toStringUtf8() }.isEqualTo("[]")
                }
            }
            withElementAt(1) {
                get { eventsList }.single().and {
                    get { id }.apply {
                        get { bookName }.isEqualTo(ROOT_EVENT_ID.bookName)
                        get { scope }.isEqualTo(ROOT_EVENT_ID.scope)
                    }
                    get { hasParentId() }.isEqualTo(true)
                    get { parentId }.isEqualTo(processorEventId)
                    get { name }.contains(Regex("Process interval \\[$ISO_DATE_TIME_REGEX - $ISO_DATE_TIME_REGEX\\)"))
                    get { type }.isEqualTo("Process interval")
                    get { body.toStringUtf8() }.isEqualTo("[]")
                }
            }
            withElementAt(2) {
                get { eventsList }.single().and {
                    get { id }.apply {
                        get { bookName }.isEqualTo(ROOT_EVENT_ID.bookName)
                        get { scope }.isEqualTo(ROOT_EVENT_ID.scope)
                    }
                    get { hasParentId() }.isEqualTo(true)
                    get { parentId }.isEqualTo(intervalEventId)
                    get { name }.isEqualTo("Complete processing")
                    get { type }.isEqualTo("Process interval")
                    get { body.toStringUtf8() }.isEqualTo("[]")
                }
            }
            withElementAt(3) {
                get { eventsList }.single().and {
                    get { id }.apply {
                        get { bookName }.isEqualTo(ROOT_EVENT_ID.bookName)
                        get { scope }.isEqualTo(ROOT_EVENT_ID.scope)
                    }
                    get { hasParentId() }.isEqualTo(true)
                    get { parentId }.isEqualTo(processorEventId)
                    get { name }.contains(Regex("Whole time range is processed \\[$ISO_DATE_TIME_REGEX - $ISO_DATE_TIME_REGEX\\)"))
                    get { type }.isEqualTo("Process interval")
                    get { body.toStringUtf8() }.isEqualTo("[]")
                }
            }

            //TODO: 4, 5 event batches are individual
        }
    }

    private fun verify() {
        verify(dataProvider, times(1)).searchMessages(any())
    }

    private fun crawlerConfiguration(messages: Boolean, events: Boolean, useTransport: Boolean) = Configuration(
        crawler = CrawlerConfiguration(
            messages = if (messages) MessageConfiguration(
                setOf(MESSAGE, RAW_MESSAGE),
                mapOf(KNOWN_BOOK to setOf())
            ) else null,
            events = if (events) EventConfiguration(mapOf(KNOWN_BOOK to setOf())) else null,
            from = FROM,
            to = TO,
            intervalLength = INTERVAL_LENGTH,
            syncInterval = INTERVAL_LENGTH.dividedBy(2),
            awaitTimeout = 1,
        ),
        stateSessionAlias = STATE_SESSION_ALIAS,
        enableStoreState = true,
        processorSettings = mock { },
        useTransport = useTransport
    )

    private fun realtimeConfiguration(useTransport: Boolean) = Configuration(
        realtime = RealtimeConfiguration(enableMessageSubscribtion = true, enableEventSubscribtion = true),
        stateSessionAlias = STATE_SESSION_ALIAS,
        enableStoreState = true,
        processorSettings = mock { },
        useTransport = useTransport,
    )

    private fun mockEvents() {
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
        private const val ISO_DATE_TIME_REGEX =
            "\\d{4}-[01]\\d-[0-3]\\dT[0-2]\\d:[0-5]\\d:[0-5]\\d\\.\\d+([+-][0-2]\\d:[0-5]\\d|Z)"

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

            override fun handle(intervalEventId: EventID, message: ProtobufRawMessage) {
                // do nothing
            }

            override fun handle(intervalEventId: EventID, message: ParsedMessage) {
                // do nothing
            }

            override fun handle(intervalEventId: EventID, message: TransportRawMessage) {
                // do nothing
            }

            override fun handle(intervalEventId: EventID, message: Message) {
                // do nothing
            }
        }
    }

}