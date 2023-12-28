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

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.Controller
import com.exactpro.th2.processor.core.message.AbstractMessageCrawler
import com.exactpro.th2.processor.core.message.transport.controller.CradleMessageGroupController
import java.time.Instant

internal class CradleMessageGroupCrawler(
    context: Context,
    processor: IProcessor,
) : AbstractMessageCrawler<GroupBatch>(
    context,
    processor,
    context.commonFactory.transportGroupBatchRouter,
) {
    override fun createController(intervalEventId: EventID, from: Instant, to: Instant): Controller<GroupBatch> {
        return CradleMessageGroupController(
            processor,
            intervalEventId,
            from,
            to,
            messageKinds,
            bookToGroups
        )
    }
}