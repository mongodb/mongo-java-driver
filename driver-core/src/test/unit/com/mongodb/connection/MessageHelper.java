/*
 * Copyright 2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
 */

package com.mongodb.connection;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.BsonInput;
import org.bson.io.OutputBuffer;
import org.bson.json.JsonReader;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.mongodb.connection.ConnectionDescription.getDefaultMaxMessageSize;

final class MessageHelper {

    private MessageHelper() {
    }

    public static ResponseBuffers buildSuccessfulReply(final String json) {
        return buildSuccessfulReply(0, json);
    }

    public static ResponseBuffers buildSuccessfulReply(final int responseTo, final String json) {
        return buildReply(responseTo, json, 0);
    }

    public static ResponseBuffers buildFailedReply(final String json) {
        return buildFailedReply(0, json);
    }

    public static ResponseBuffers buildFailedReply(final int responseTo, final String json) {
        return buildReply(responseTo, json, 2);
    }

    public static ResponseBuffers buildReply(final int responseTo, final String json, final int responseFlags) {
        ByteBuf body = encodeJson(json);
        body.flip();

        ReplyHeader header = buildReplyHeader(responseTo, 1, body.remaining(), responseFlags);
        return new ResponseBuffers(header, body);
    }

    private static ReplyHeader buildReplyHeader(final int responseTo, final int numDocuments, final int documentsSize,
                                                final int responseFlags) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(36 + documentsSize); // length
        headerByteBuffer.putInt(2456); //request id
        headerByteBuffer.putInt(responseTo); // response to
        headerByteBuffer.putInt(1); // opcode
        headerByteBuffer.putInt(responseFlags); // responseFlags
        headerByteBuffer.putLong(0); // cursorId
        headerByteBuffer.putInt(0); // startingFrom
        headerByteBuffer.putInt(numDocuments); //numberReturned
        headerByteBuffer.flip();

        ByteBufNIO buffer = new ByteBufNIO(headerByteBuffer);
        return new ReplyHeader(buffer, new MessageHeader(buffer, getDefaultMaxMessageSize()));
    }

    public static BsonDocument decodeCommand(final BsonInput bsonInput) {
        bsonInput.readInt32(); // length
        bsonInput.readInt32(); //requestId
        bsonInput.readInt32(); //responseTo
        bsonInput.readInt32(); // opcode
        bsonInput.readInt32(); // flags
        bsonInput.readCString(); //collectionName
        bsonInput.readInt32(); // numToSkip
        bsonInput.readInt32(); // numToReturn

        BsonBinaryReader reader = new BsonBinaryReader(bsonInput);
        return new BsonDocumentCodec().decode(reader, DecoderContext.builder().build());
    }

    public static String decodeCommandAsJson(final BsonInput bsonInput) {
        return decodeCommand(bsonInput).toJson();
    }

    private static ByteBuf encodeJson(final String json) {
        OutputBuffer outputBuffer = new BasicOutputBuffer();
        JsonReader jsonReader = new JsonReader(json);
        BsonDocumentCodec codec = new BsonDocumentCodec();
        BsonDocument document = codec.decode(jsonReader, DecoderContext.builder().build());
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        codec.encode(writer, document, EncoderContext.builder().build());

        ByteBuffer documentByteBuffer = ByteBuffer.allocate(outputBuffer.size());
        documentByteBuffer.put(outputBuffer.toByteArray());
        return new ByteBufNIO(documentByteBuffer);
    }
}
