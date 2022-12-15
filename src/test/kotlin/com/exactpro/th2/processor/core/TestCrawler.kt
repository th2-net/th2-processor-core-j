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

package com.exactpro.th2.processor.core

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.ExclusiveSubscriberMonitor
import com.exactpro.th2.processor.core.configuration.Configuration
import com.google.protobuf.Message
import com.google.protobuf.Timestamp
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import java.time.Instant

class TestCrawler {

    @Test
    fun `init crawler`() {
        val monitor = mock<ExclusiveSubscriberMonitor> {
            on { queue }.thenReturn("test-queue")
        }

        assertDoesNotThrow {
            object : Crawler<Message>(
                mock {  },
                EventID.getDefaultInstance(),
                mock {
                    on { subscribeExclusive(any()) }.thenReturn(monitor)
                } ,
                Configuration(
                    from = Instant.now().toString(),
                    processorSettings = mock {  }
                ),
                mock {  }
            ) {
                override fun process(from: Timestamp, to: Timestamp, intervalEventId: EventID) {
                    // do nothing
                }
            }
        }
    }
}