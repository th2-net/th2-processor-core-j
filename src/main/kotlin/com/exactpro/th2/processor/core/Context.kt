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

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.schema.factory.CommonFactory
import com.exactpro.th2.common.utils.event.EventBatcher
import com.exactpro.th2.processor.api.IProcessorFactory
import com.exactpro.th2.processor.core.configuration.Configuration
import com.exactpro.th2.processor.core.state.IStateStorage
import java.util.concurrent.ScheduledExecutorService

class Context(
    val commonFactory: CommonFactory,
    val processorFactory: IProcessorFactory,

    val processorEventId: EventID,
    val stateStorage: IStateStorage,

    val eventBatcher: EventBatcher,
    val scheduler: ScheduledExecutorService,
    val configuration: Configuration,
)
