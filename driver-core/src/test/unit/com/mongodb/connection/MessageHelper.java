package com.mongodb.connection;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.InputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;

import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

final class MessageHelper {

    private MessageHelper() {
    }

    public static ResponseBuffers buildSuccessfulReply(final String json) {
        return buildSuccessfulReply(0, json);
    }

    public static ResponseBuffers buildSuccessfulReply(final int responseTo, final String json) {
        ByteBuf body = encodeJson(json);
        body.flip();

        ReplyHeader header = buildSuccessfulReplyHeader(responseTo, 1, body.remaining());
        return new ResponseBuffers(header, body);
    }

    private static ReplyHeader buildSuccessfulReplyHeader(final int responseTo, final int numDocuments, final int documentsSize) {
        ByteBuffer headerByteBuffer = ByteBuffer.allocate(36);
        headerByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        headerByteBuffer.putInt(36 + documentsSize); // length
        headerByteBuffer.putInt(2456); //request id
        headerByteBuffer.putInt(responseTo); // response to
        headerByteBuffer.putInt(1); // opcode
        headerByteBuffer.putInt(0); // responseFlags
        headerByteBuffer.putLong(0); // cursorId
        headerByteBuffer.putInt(0); // startingFrom
        headerByteBuffer.putInt(numDocuments); //numberReturned
        headerByteBuffer.flip();

        BasicInputBuffer headerInputBuffer = new BasicInputBuffer(new ByteBufNIO(headerByteBuffer));
        return new ReplyHeader(headerInputBuffer);
    }

    public static String decodeCommandAsJson(final InputBuffer inputBuffer) {
        inputBuffer.readInt32(); // length
        inputBuffer.readInt32(); //requestId
        inputBuffer.readInt32(); //responseTo
        inputBuffer.readInt32(); // opcode
        inputBuffer.readInt32(); // flags
        inputBuffer.readCString(); //collectionName
        inputBuffer.readInt32(); // numToSkip
        inputBuffer.readInt32(); // numToReturn

        BsonBinaryReader reader = new BsonBinaryReader(inputBuffer, true);
        BsonDocumentCodec codec = new BsonDocumentCodec();
        BsonDocument document = codec.decode(reader, DecoderContext.builder().build());
        StringWriter writer = new StringWriter();
        JsonWriter jsonWriter = new JsonWriter(writer);
        codec.encode(jsonWriter, document, EncoderContext.builder().build());
        return writer.toString();
    }

    private static ByteBuf encodeJson(final String json) {
        OutputBuffer outputBuffer = new BasicOutputBuffer();
        JsonReader jsonReader = new JsonReader(json);
        BsonDocumentCodec codec = new BsonDocumentCodec();
        BsonDocument document = codec.decode(jsonReader, DecoderContext.builder().build());
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer, true);
        codec.encode(writer, document, EncoderContext.builder().build());

        ByteBuffer documentByteBuffer = ByteBuffer.allocate(outputBuffer.size());
        documentByteBuffer.put(outputBuffer.toByteArray());
        return new ByteBufNIO(documentByteBuffer);
    }
}
