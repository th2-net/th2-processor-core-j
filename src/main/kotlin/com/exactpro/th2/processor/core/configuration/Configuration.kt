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

package com.exactpro.th2.processor.core.configuration

import com.exactpro.th2.processor.api.IProcessorSettings
import java.util.concurrent.TimeUnit

class Configuration @JvmOverloads constructor(

    val messages: MessageConfiguration? = null,
    val events: EventConfiguration? = null,
    /**
     * Name of th2 session alias for storing/restoring state. th2 box name will be used if the value is blank.
     */
    val stateSessionAlias: String = "",
    val enableStoreState: Boolean = false,

    val bookName: String,

    val to: String? = null,
    val from: String,
    val intervalLength: String = "PT10M",
    val syncInterval: String = "PT10M",

    val awaitTimeout: Long = 10,
    val awaitUnit: TimeUnit = TimeUnit.SECONDS,

    val processorSettings: IProcessorSettings
)
