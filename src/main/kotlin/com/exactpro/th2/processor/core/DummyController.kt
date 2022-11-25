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

import com.google.protobuf.Message
import com.google.protobuf.TextFormat.shortDebugString
import mu.KotlinLogging

class DummyController<T: Message> : Controller<T>() {

    override val isStateEmpty: Boolean = true

    override fun actual(batch: T) {
        K_LOGGER.debug { "Skip ${shortDebugString(batch)}" }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {}
    }
}