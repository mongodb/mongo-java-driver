/*
 * Copyright 2008-2017 MongoDB Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.mongodb.connection

import com.mongodb.MongoNamespace
import com.mongodb.async.FutureResultCallback
import org.bson.BsonBinaryWriter
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonReader
import org.bson.BsonWriter
import org.bson.ByteBuf
import org.bson.ByteBufNIO
import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
import org.bson.json.JsonReader

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.security.SecureRandom

import static com.mongodb.MongoNamespace.COMMAND_COLLECTION_NAME

class StreamHelper {
    private static int nextMessageId = 900000 // Generates a message then adds one to the id
    private static final DEFAULT_JSON_RESPONSE = '{connectionId: 1,   n: 0,   syncMillis: 0,   writtenTo:  null,  err: null,   ok: 1 }'

    static write(List<ByteBuf> buffers) {
        buffers.each {
            it.get(new byte[it.remaining()])
        }
    }

    static read(List<Integer> messageIds) {
        read(messageIds, true)
    }

    static read(List<Integer> messageIds, boolean ordered) {
        List<ByteBuf> headers = messageIds.collect { defaultHeader(it) }
        List<ByteBuf> bodies = messageIds.collect { defaultBody() }
        if (!ordered) {
            Collections.shuffle(headers, new SecureRandom())
        }
        [headers, bodies].transpose().flatten()
    }

    static defaultHeader(messageId) {
        header(messageId, DEFAULT_JSON_RESPONSE)
    }

    static header(messageId, json) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36).with {
            order(ByteOrder.LITTLE_ENDIAN)
            putInt(36 + body(json).remaining()) // messageLength
            putInt(4)                     // requestId
            putInt(messageId)                   // responseTo
            putInt(1)                     // opCode
            putInt(0)                     // responseFlags
            putLong(0)                    // cursorId
            putInt(0)                     // starting from
            putInt(1)                     // number returned
        }
        headerByteBuffer.flip()
        new ByteBufNIO(headerByteBuffer)
    }

    static headerWithMessageSizeGreaterThanMax(messageId, maxMessageSize) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36).with {
            order(ByteOrder.LITTLE_ENDIAN)
            putInt(maxMessageSize + 1)  // messageLength
            putInt(4)             // requestId
            putInt(messageId)           // responseTo
            putInt(1)             // opCode
            putInt(0)             // responseFlags
            putLong(0)            // cursorId
            putInt(0)             // starting from
            putInt(1)             // number returned
        }
        headerByteBuffer.flip()
        new ByteBufNIO(headerByteBuffer)
    }

    static defaultBody() {
        body(DEFAULT_JSON_RESPONSE)
    }

    static body(json) {
        BsonReader reader = new JsonReader(json)
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer()
        BsonWriter writer = new BsonBinaryWriter(outputBuffer)
        writer.pipe(reader)
        new ByteBufNIO(ByteBuffer.allocate(outputBuffer.size()).put(outputBuffer.toByteArray())).flip()
    }

    static generateHeaders(List<Integer> messageIds) {
        generateHeaders(messageIds, true)
    }

    static generateHeaders(List<Integer> messageIds, boolean ordered) {
        List<ByteBuf> headers = messageIds.collect { defaultHeader(it) }
        if (!ordered) {
            Collections.shuffle(headers, new SecureRandom())
        }
        headers
    }

    static isMaster() {
        CommandMessage command = new SimpleCommandMessage(new MongoNamespace('admin', COMMAND_COLLECTION_NAME).getFullName(),
                new BsonDocument('ismaster', new BsonInt32(1)),
                false, MessageSettings.builder().build())
        OutputBuffer outputBuffer = new BasicOutputBuffer()
        command.encode(outputBuffer)
        nextMessageId++
        [outputBuffer.byteBuffers, nextMessageId]
    }

    static isMasterAsync() {
        isMaster() + [new FutureResultCallback<Void>(), new FutureResultCallback<ResponseBuffers>()]
    }
}