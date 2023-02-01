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

package com.exactpro.th2.processor.strategy

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.metrics.registerReadiness
import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.api.ProcessorContext
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.utility.OBJECT_MAPPER

abstract class AbstractStrategy(
    protected val context: Context
): AutoCloseable {
    @Volatile
    private var _isActive = true
    protected val isActive
        get() = _isActive

    protected val readiness = registerReadiness("strategy")

    open fun run() {}

    override fun close() {
        readiness.disable()
        _isActive = false
    }

    protected fun storeState(parentEventId: EventID, state: Any) {
        with(context) {
            if (configuration.enableStoreState) {
                OBJECT_MAPPER.writeValueAsBytes(state).also { rawData ->
                    stateStorage.saveState(parentEventId, rawData)
                }
            }
        }
    }

    protected fun <T> recoverState(stateClass: Class<T>): T? {
        with(context) {
            if (configuration.enableStoreState) {
                stateStorage.loadState(processorEventId)?.let { rawData ->
                    runCatching {
                        OBJECT_MAPPER.readValue(rawData, stateClass)
                    }.onFailure { e ->
                        throw IllegalStateException("State of $stateClass class can't be decode from " +
                            rawData.joinToString("") { it.toString(radix = 16).padStart(2, '0') }, e)
                    }.getOrThrow()
                }
            }
            return null
        }
    }
    protected fun createProcessor(
        processorState: ByteArray?
    ): IProcessor = runCatching {
        with(context) {
            processorFactory.create(
                ProcessorContext(
                    commonFactory,
                    scheduler,
                    eventBatcher,
                    processorEventId,
                    configuration.processorSettings,
                    processorState
                )
            )
        }
    }.getOrElse {
        throw IllegalStateException("Failed to create processor instance", it)
    }
}