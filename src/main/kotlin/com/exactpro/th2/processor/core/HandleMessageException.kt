/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.utils.message.transport.toProto

open class HandleMessageException : ProcessorException {
    val messageIds: List<MessageID>

    constructor(
        messageIds: List<MessageID>,
        message: String? = null,
        cause: Throwable? = null,
        enableSuppression: Boolean = true,
        writableStackTrace: Boolean = true
    ) : super(
        message,
        cause,
        enableSuppression,
        writableStackTrace,
    ) {
        this.messageIds = messageIds
    }

    constructor(
        book: String,
        sessionGroup: String,
        messageIds: List<MessageId>,
        message: String? = null,
        cause: Throwable? = null,
        enableSuppression: Boolean = true,
        writableStackTrace: Boolean = true
    ) : this(messageIds.map { it.toProto(book, sessionGroup) }, message, cause, enableSuppression, writableStackTrace)
}