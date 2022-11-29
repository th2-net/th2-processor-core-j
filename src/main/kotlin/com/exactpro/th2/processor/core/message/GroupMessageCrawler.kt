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

package com.exactpro.th2.processor.core.message

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsQueueSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsQueueSearchRequest.BookGroups
import com.exactpro.th2.dataprovider.lw.grpc.QueueDataProviderService
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.Crawler
import com.exactpro.th2.processor.core.message.controller.GroupController
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.Timestamp
import mu.KotlinLogging

internal class GroupMessageCrawler(
    context: Context,
) : Crawler<MessageGroupBatch>(
    context.messageRouter,
    context.configuration,
    context.processor
) {
    private val dataProvider: QueueDataProviderService = context.dataProvider

    private val messageKind = requireNotNull(context.configuration.messages).messageKind

    private val bookToGroups = requireNotNull(
        requireNotNull(context.configuration.messages).bookToGroups
    ).also {
        check(it.isNotEmpty()) {
            "Incorrect configuration parameters: the `bookToGroups` option is empty"
        }
    }

    private val bookGroups = bookToGroups.map { (book, groups) ->
        BookGroups.newBuilder().apply {
            bookIdBuilder.apply { name = book }
            groups.forEach { group ->
                addGroupBuilder().apply { name = group }
            }
        }.build()
    }

    override fun process(from: Timestamp, to: Timestamp) {
        controller = GroupController(processor, from, to, messageKind, bookToGroups)

        val request = MessageGroupsQueueSearchRequest.newBuilder().apply {
            startTimestamp = from
            endTimestamp = to
            externalQueue = queue

            addAllMessageGroup(bookGroups)
        }.build()

        K_LOGGER.info { "Request ${shortDebugString(request)}" }
        dataProvider.searchMessageGroups(request)
            .also { response ->
                K_LOGGER.info { "Request ${shortDebugString(response)}" }
                controller.expected(response)
                check(controller.await(awaitTimeout, awaitUnit)) {
                    "Quantification failure after ($awaitTimeout:$awaitUnit waiting, controller $controller)"
                }
            }
    }
    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}