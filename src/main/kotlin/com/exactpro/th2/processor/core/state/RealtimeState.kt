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

package com.exactpro.th2.processor.core.state

import com.fasterxml.jackson.annotation.JsonProperty

data class RealtimeState(
    @JsonProperty("processorState") val processorState: ByteArray?
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as RealtimeState

        if (processorState != null) {
            if (other.processorState == null) return false
            if (!processorState.contentEquals(other.processorState)) return false
        } else if (other.processorState != null) return false

        return true
    }

    override fun hashCode(): Int {
        return processorState?.contentHashCode() ?: 0
    }

}
