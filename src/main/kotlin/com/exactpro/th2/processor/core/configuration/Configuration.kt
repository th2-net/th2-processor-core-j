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

import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.api.IProcessorSettings
import com.exactpro.th2.processor.utility.load
import com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import java.util.concurrent.TimeUnit

class Configuration @JvmOverloads constructor(

    val type: DataType = DataType.MESSAGE_GROUP,
    /**
     * Name of th2 session alias for storing/restoring state. th2 box name will be used if the value is blank.
     */
    val stateSessionAlias: String,

    val to: String?,
    val from: String,
    val intervalLength: String = "PT10M",
    val th2Groups: List<String> = emptyList(),

    val awaitTimeout: Long = 10,
    val awaitUnit: TimeUnit = TimeUnit.SECONDS,

    val IProcessorSettings: IProcessorSettings
) {
    companion object {
        private val OBJECT_MAPPER: ObjectMapper = ObjectMapper(YAMLFactory()).apply {
            registerKotlinModule()
            registerModule(SimpleModule().addAbstractTypeMapping(IProcessorSettings::class.java, load<IProcessorFactory>().settingsClass))
            configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        }

        fun create(commonFactory: CommonFactory): Configuration {
            return commonFactory.getCustomConfiguration(Configuration::class.java, OBJECT_MAPPER)
        }
    }
}
