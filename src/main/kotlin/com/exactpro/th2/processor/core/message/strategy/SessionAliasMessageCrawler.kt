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

package com.exactpro.th2.processor.core.message.strategy

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.configuration.Configuration
import com.google.protobuf.Timestamp

internal class SessionAliasMessageCrawler (
    messageRouter: MessageRouter<MessageGroupBatch>,
    dataProvider: QueueDataProviderService,
    processor: IProcessor,
    configuration: Configuration
) : MessageCrawler(
    messageRouter,
    dataProvider,
    processor,
    configuration
) {
    private val bookToSessionAliases = requireNotNull(
        requireNotNull(configuration.messages).bookToSessionAliases
    ).also {
        check(it.isNotEmpty()) {
            "Incorrect configuration parameters: the `bookToSessionAliases` option is empty"
        }
    }

    override fun processInterval(from: Timestamp, to: Timestamp) {
        TODO("Not yet implemented")
    }
}