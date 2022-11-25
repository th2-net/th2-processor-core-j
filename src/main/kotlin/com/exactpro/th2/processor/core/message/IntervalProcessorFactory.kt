/*
 *  Copyright 2022 Exactpro (Exactpro Systems Limited)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.exactpro.th2.processor.core.message

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.message.strategy.GroupMessageCrawler
import com.exactpro.th2.processor.core.message.strategy.MessageCrawler
import com.exactpro.th2.processor.core.message.strategy.SessionAliasMessageCrawler

fun createMessageIntervalProcessor (
    messageRouter: MessageRouter<MessageGroupBatch>,
    dataProvider: QueueDataProviderService,
    configuration: Configuration,
    processor: IProcessor,
): MessageCrawler {
    val messageConfiguration = requireNotNull(configuration.messages) {
        "Incorrect configuration parameters: the `messages` configuration can not be null"
    }

    with(messageConfiguration) {
        if (bookToGroups != null && bookToSessionAliases != null) {
            error("Incorrect configuration parameters: " +
                    "Both of `bookToGroups`, `bookToSessionAliases` options are filled. " +
                    "Processor can work in one mode only")
        }

        return when {
            bookToGroups != null -> {
                GroupMessageCrawler(messageRouter, dataProvider, processor, configuration)
            }
            bookToSessionAliases != null -> {
                SessionAliasMessageCrawler(messageRouter, dataProvider, processor, configuration)
            }
            else -> error("Incorrect configuration parameters: " +
                    "Neither of `bookToGroups`, `bookToSessionAliases` options are filled. " +
                    "Processor should work in one modes")
        }
    }
}