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
package com.exactpro.th2.processor.core.state

import com.exactpro.th2.common.grpc.Direction.SECOND
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.dataprovider.grpc.DataProviderService
import com.exactpro.th2.dataprovider.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.grpc.TimeRelation.PREVIOUS
import com.google.protobuf.Int32Value
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import mu.KotlinLogging
import java.time.Instant

class MQStateStorage(
    private val dataProvider: DataProviderService,
    stateSessionAlias: String
) : IStateStorage {

    private val requestBuilder = MessageSearchRequest.newBuilder().apply {
        searchDirection = PREVIOUS
        resultCountLimit = Int32Value.of(COUNT_LIMIT)
        addResponseFormats(RAW_MESSAGE_RESPONSE_FORMAT)
        addStreamBuilder().apply {
            name = stateSessionAlias
            direction = SECOND
        }
    }

    override fun loadState(): ByteArray? {
        K_LOGGER.info { "Started state loading" }

        val state = mutableListOf<RawMessage>()
        sequence {
            var iterator = dataProvider.searchMessages(createSearchRequest())

            while (iterator.hasNext()) {
                val message = iterator.next().message.rawMessage
                yield(message)
                if (!iterator.hasNext()) {
                    iterator = dataProvider.searchMessages(createSearchRequest(message.metadata.id.timestamp))
                }
            }
        }.forEach { message ->
            val stateType = message.metadata.propertiesMap[StateType.METADATA_STATE_TYPE_PROPERTY]

//            when {
//                state.isEmpty() && (stateType == END.name || stateType == SINGLE.name) -> {
//
//                }
//                state.isNotEmpty() && () -> {
//
//                }
//            }
        }

        return null



//        val processorState = try {
//            val state = mutableListOf<RawMessage>()
//            val request = createSearchRequest()
//
//
//
//
//            K_LOGGER.info { "Send state request ${shortDebugString(request)}" }
//            var iterator = dataProvider.searchMessages(request)
//            K_LOGGER.debug { "State response has got for request ${shortDebugString(request)}" }
//
//            if (!iterator.hasNext()) {
//                return null
//            }
//
//            var isReadingStarted = false
//            var lastSequence: Long? = null
//            while (!readStates(state, iterator, isReadingStarted, lastSequence)) {
//                isReadingStarted = true
//                val searchRequest = createSearchRequest(state.last().metadata.timestamp)
//                lastSequence = state.last().sequence
//                LOGGER.info { "Querying cradle for state messages." }
//                iterator = checkNotNull(dataProvider.searchMessages(searchRequest)) {
//                    "Data provider `search` call returned null iterator. Search request: ${
//                        shortDebugString(
//                            searchRequest
//                        )
//                    }"
//                }
//                LOGGER.debug { "Data provider query finished." }
//            }
//
//            LOGGER.info { "State loaded. Size ${state.size}" }
//
//            return ProcessorState(
//                state.asReversed().map { it.toDriedMessage() }
//            ).also { LOGGER.debug { "State: ${it.stateMessages.joinToString("\n")}" } }
//
//            dataProviderStateExtractor.loadState()
//        } catch (e: Exception) {
//            K_LOGGER.error { "Error while loading state: ${e.message}" }
//            sendEvent("Error while loading state: ${e.message}", EventType.STATE_MANIPULATION_ERROR)
//            throw e
//        }
//        K_LOGGER.info { "Feeding loaded state to rules." }
//        messageHandler.handle(processorState.stateMessages)
//        return processorState.stateMessages.lastOrNull()?.let { ProcessorStateMarker(it, ::sendEvent) }
//            ?: ProcessorStateMarker.EMPTY_MARK
    }

    override fun saveState(state: ByteArray) {
        TODO("Not yet implemented")
    }

//    override fun saveState(state: ByteArray) {
//        K_LOGGER.info { "Collecting state messages." }
//        val state = ruleStateExtractor.collectState(crawlerInterval)
//        K_LOGGER.info { "Pushing state messages to message queue." }
//        try {
//            mqSaver.saveStates(state, crawlerInterval)
//        } catch (e: Exception) {
//            K_LOGGER.error { "Error while saving state: ${e.message}" }
//            sendEvent("Error while saving state: ${e.message}", EventType.STATE_MANIPULATION_ERROR)
//        }
//    }
//
//    private fun sendEvent(name: String, type: EventType) {
//        eventRouter.sendAll(Event.start().name(name).type(type).toBatchProto(rootEventId))
//    }
//
//    private fun readStates(result: MutableList<RawMessage>,
//                           iterator: Iterator<MessageSearchResponse>,
//                           isReadingStarted: Boolean = false,
//                           lastSequence: Long?
//    ): Boolean {
//        var startCollectingMessages = false
//        val startStateSize = result.size
//        var lastSequence = lastSequence
//        while (iterator.hasNext()) {
//            val message = iterator.next()
//            K_LOGGER.debug { "State message is: $message" }
//            val current = message.message.rawMessage
//            val body = current.body
//
//            if(body.size() == 0) {
//                K_LOGGER.warn { "State message had body with size = 0. Message id: ${current.metadata.id}" }
//                continue
//            }
//
//            val metadata = current.metadata
//
//            if(lastSequence == null) {
//                lastSequence = current.sequence
//            } else {
//                val currentSequence = current.sequence
//                if(currentSequence >= lastSequence) {
//                    continue
//                }
//                check(lastSequence - 1 == currentSequence) {
//                    "Detected sequence gap. Previous sequence: $lastSequence Current sequence: ${current.sequence}"
//                }
//            }
//
//            val properties = metadata.propertiesMap
//            val stateType = properties[METADATA_STATE_TYPE_PROPERTY]
//
//            if(stateType == StateType.START.value || stateType == SINGLE.value) {
//                result.add(current)
//                return true
//            }
//
//            if(stateType == END.value || stateType == SINGLE.value) {
//                startCollectingMessages = true
//            }
//
//            if(startCollectingMessages || isReadingStarted) {
//                result.add(current)
//            }
//        }
//        if(startStateSize == result.size) return true
//        return false
//    }

    private fun createSearchRequest(timestamp: Timestamp = Instant.now().toTimestamp()): MessageSearchRequest =
        requestBuilder.apply {
            startTimestamp = timestamp
        }.build()

    private enum class StateType {
        START,
        END,
        SINGLE,
        INTERMEDIATE;

        companion object {
            const val METADATA_STATE_TYPE_PROPERTY = "stateType"
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger { }

        private const val RAW_MESSAGE_RESPONSE_FORMAT = "BASE_64"
        private const val COUNT_LIMIT = 100

        private fun min(t1: Timestamp, t2: Timestamp) = if (Timestamps.compare(t1, t2) > 0) t1 else t2
    }
}