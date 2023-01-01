package com.exactpro.th2.processor.core.state.manager

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageGroupBatch
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.utils.state.StateManager
import com.exactpro.th2.dataprovider.grpc.MessageSearchResponse
import com.exactpro.th2.processor.utility.toByteArray
import java.time.Instant

class MessageStateManager(
    private val loadLimit: Int,
    private val storeLimit: Int,
    private val onEvent: (batch: EventBatch) -> Unit,
    private val processorEventID: EventID,
    private val loadRawMessages: (from: Instant, to: Instant, direction: Direction, stateSessionAlise: String, limit: Int, bookId: String) -> Iterator<MessageSearchResponse>,
    private val storeRawMessages: (batch: MessageGroupBatch) -> Unit,
) : StateManager {
    override fun store(rawData: ByteArray, stateSessionAlise: String) {
        storeRawMessages(MessageGroupBatch.newBuilder().mergeFrom(rawData).build())
    }

    // FIXME: probably it's better to implement it another way
    override fun load(from: Instant, to: Instant, direction: Direction, stateSessionAlise: String, bookId: String): ByteArray? {
        val res = ArrayList<RawMessage>()
        val iterator = loadRawMessages(from, to, direction, stateSessionAlise, loadLimit, bookId)

        while (iterator.hasNext()) {
            res.add(iterator.next().message.rawMessage)
        }

        return if (res.isEmpty()) null else res.toByteArray()
    }
}