/*
 *  Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.processor.core.configuration

import com.exactpro.th2.processor.Application
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

class CrawlerConfiguration @JvmOverloads constructor(
    val messages: MessageConfiguration? = null,
    val events: EventConfiguration? = null,

    val to: Instant? = null,
    val from: Instant,
    val intervalLength: Duration = Duration.ofMinutes(10),
    val syncInterval: Duration = Duration.ofMinutes(10),

    val awaitTimeout: Long = 10,
    val awaitUnit: TimeUnit = TimeUnit.SECONDS,
) {
    init {
        check(!intervalLength.isNegative && !intervalLength.isZero) {
            Application.CONFIGURATION_ERROR_PREFIX +
                    "the $intervalLength `interval length` option is negative or zero"
        }
        check(!syncInterval.isNegative && !syncInterval.isZero) {
            Application.CONFIGURATION_ERROR_PREFIX +
                    "the $syncInterval `synchronize interval` option is negative or zero"
        }
        check(syncInterval <= intervalLength) {
            Application.CONFIGURATION_ERROR_PREFIX +
                    "the $syncInterval `synchronize interval` option is greater than the $intervalLength `interval length`"
        }
        check(to == null || to >= from) {
            Application.CONFIGURATION_ERROR_PREFIX +
                    "the $to `to` option is less than the $from `from`"
        }
        check(awaitTimeout > 0) {
            Application.CONFIGURATION_ERROR_PREFIX +
                    "the $awaitTimeout `await timeout` option isn't positive"
        }
        check((messages != null) || (events != null)) {
            Application.CONFIGURATION_ERROR_PREFIX +
                    "neither of $messages `messages`, $events `events` options are filled."
        }
    }
}
