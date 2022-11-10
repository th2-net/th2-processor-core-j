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

package com.exactpro.th2.processor.core.message.controller

import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.schema.message.toShortDebugString
import com.exactpro.th2.dataprovider.grpc.MessageIntervalInfo
import mu.KotlinLogging
import java.util.concurrent.TimeUnit

class DummyMessageController : IMessageController {
    override fun actual(batch: MessageGroupBatch) {
        K_LOGGER.debug { "Skip ${batch.toShortDebugString()}" }
    }

    override fun expected(intervalInfo: MessageIntervalInfo) {
        // do noting
    }

    override fun await(time: Long, unit: TimeUnit): Boolean = true

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}

        val INSTANT = DummyMessageController()
    }
}