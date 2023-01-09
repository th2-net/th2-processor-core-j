package com.exactpro.th2.processor.core.state.manager

import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.utils.state.StateManager
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.processor.utility.toByteArray
import com.google.protobuf.ByteString

class MessageStateManager(
    private val statePartSize: Long,
    private val onEvent: (event: Event) -> Unit,
//    private val loadRawMessages: (bookId: String, sessionAlias: String) -> Iterator<RawMessage>,
    private val loadRawMessages: (bookId: String, sessionAlias: String) -> Iterator<MessageSearchResponse>,
    private val storeRawMessage: (statePart: RawMessage) -> Unit,
) : StateManager {
    override fun store(rawData: ByteArray, stateSessionAlias: String, bookId: String) {
        storeRawMessage(RawMessage.newBuilder().setBody(ByteString.copyFrom(rawData)).build()) // FIXME: ???
    }

    // TODO: possibly load(...) should return Iterator<RawMessage>
    override fun load(stateSessionAlias: String, bookId: String): ByteArray? {
        val res = ArrayList<RawMessage>()
        val iterator = loadRawMessages(bookId, stateSessionAlias)

        while (iterator.hasNext()) {
            res.add(iterator.next().message.rawMessage)
        }

        return if (res.isEmpty()) null else res.toByteArray()
    }
}