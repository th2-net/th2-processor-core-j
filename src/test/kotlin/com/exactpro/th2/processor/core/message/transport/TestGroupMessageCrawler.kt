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

package com.exactpro.th2.processor.core.message.transport

import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.configuration.CrawlerConfiguration
import com.exactpro.th2.processor.core.configuration.MessageConfiguration
import com.exactpro.th2.processor.core.configuration.MessageKind
import com.exactpro.th2.processor.core.message.AbstractGroupMessageCrawlerTest
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever

internal class TestCradleMessageGroupCrawler: AbstractGroupMessageCrawlerTest<GroupBatch>() {
    private val messageRouter = mock<MessageRouter<GroupBatch>> {
        on { subscribeExclusive(any()) }.thenReturn(monitor)
    }

    init {
        whenever(commonFactory.transportGroupBatchRouter).thenReturn(messageRouter)
    }

    override fun createCrawler(kinds: Set<MessageKind>): CradleMessageGroupCrawler {
        whenever(context.configuration).thenReturn(
            Configuration(
                crawler = CrawlerConfiguration(
                    messages = MessageConfiguration(
                        kinds,
                        mapOf(BOOK_NAME to setOf())
                    ),
                    from = FROM,
                    to = TO,
                ),
                processorSettings = mock {  }
            )
        )
        return CradleMessageGroupCrawler(context, processor)
    }
}