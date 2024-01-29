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

package com.exactpro.th2.processor.strategy.transport

import com.exactpro.th2.processor.api.IProcessor
import com.exactpro.th2.processor.core.Context
import com.exactpro.th2.processor.core.message.transport.CradleMessageGroupCrawler
import com.exactpro.th2.processor.strategy.AbstractCrawlerStrategy

class CrawlerStrategy(context: Context): AbstractCrawlerStrategy(context) {
    override fun createCradleMessageGroupCrawler(context: Context, processor: IProcessor) = CradleMessageGroupCrawler(context, processor)
}