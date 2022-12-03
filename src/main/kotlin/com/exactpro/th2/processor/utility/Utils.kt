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

package com.exactpro.th2.processor.utility

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.cbor.databind.CBORMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps

val OBJECT_MAPPER: ObjectMapper = CBORMapper()
    .registerModule(JavaTimeModule())

inline fun Boolean.ifTrue(func: () -> Unit): Boolean = this.also { if(it) func() }
inline fun Boolean.ifFalse(func: () -> Unit): Boolean = this.also { if(!it) func() }

//TODO: move to common-util
operator fun Timestamp.compareTo(another: Timestamp): Int = Timestamps.compare(this, another)