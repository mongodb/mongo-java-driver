package org.mongodb.codecs;

import org.bson.BSONBinaryReader;
import org.bson.BSONBinaryWriter;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.mongodb.Codec;
import org.mongodb.Document;

import java.nio.ByteBuffer;

public final class CodecTestUtil {
    private CodecTestUtil() { }

    static BSONBinaryReader prepareReaderWithObjectToBeDecoded(final Object objectToDecode) {
        return prepareReaderWithObjectToBeDecoded(objectToDecode, Codecs.createDefault());
    }

    static BSONBinaryReader prepareReaderWithObjectToBeDecoded(final Object objectToDecode, final Codecs codecs) {
        //Need to encode it wrapped in a document to conform to the validation
        final Document document = new Document("wrapperDocument", objectToDecode);

        final BasicOutputBuffer outputBuffer = new BasicOutputBuffer();

        codecs.encode(new BSONBinaryWriter(outputBuffer), document);
        final byte[] documentAsByteArrayForReader = outputBuffer.toByteArray();

        final BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(ByteBuffer.wrap(documentAsByteArrayForReader)));

        //have to read off the wrapper document so the reader is in the correct position for the test
        reader.readStartDocument();
        reader.readName();
        return reader;
    }

    static <T> BSONBinaryReader prepareReaderWithObjectToBeDecoded(final T objectToDecode, final Codec<T> codec) {
        final BasicOutputBuffer outputBuffer = new BasicOutputBuffer();

        codec.encode(new BSONBinaryWriter(outputBuffer), objectToDecode);
        final byte[] documentAsByteArrayForReader = outputBuffer.toByteArray();

        final BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(ByteBuffer.wrap(documentAsByteArrayForReader)));

        return reader;
    }
}
