package com.exactpro.th2.util

import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation
import com.exactpro.th2.processor.Application
import com.exactpro.th2.processor.core.configuration.Configuration
import com.google.protobuf.Int32Value
import com.google.protobuf.Timestamp
import java.time.Instant

private const val RAW_MESSAGE_RESPONSE_FORMAT = "BASE_64"
private const val COUNT_LIMIT = 100
fun createSearchRequest(configuration: Configuration, timestamp: Timestamp = Instant.now().toTimestamp()): MessageSearchRequest =
    requestBuilder(configuration).apply {
        startTimestamp = timestamp
    }.build()

private fun requestBuilder(configuration: Configuration) = MessageSearchRequest.newBuilder().apply {
    searchDirection = TimeRelation.PREVIOUS
    resultCountLimit = Int32Value.of(COUNT_LIMIT)
    addResponseFormats(RAW_MESSAGE_RESPONSE_FORMAT)
    bookIdBuilder.apply {
        name = configuration.bookName
    }
    addStreamBuilder().apply {
        name = configuration.stateSessionAlias
        direction = Direction.SECOND
    }
}
