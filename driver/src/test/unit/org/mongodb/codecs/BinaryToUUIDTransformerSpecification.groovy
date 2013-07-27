package org.mongodb.codecs

import org.bson.BSONBinaryReader
import org.bson.ByteBufNIO
import org.bson.io.BasicInputBuffer
import org.bson.types.Binary
import spock.lang.Specification
import spock.lang.Subject

import static java.nio.ByteBuffer.wrap

class BinaryToUUIDTransformerSpecification extends Specification {

    @Subject
    private final BinaryToUUIDTransformer binaryToUUIDTransformer = new BinaryToUUIDTransformer();

    def 'should read little endian encoded longs'() {
        given:
        byte[] binaryTypeWithUUIDAsBytes = [
            0, 0, 0, 0,            // document
            5,                      // type (BINARY)
            95, 105, 100, 0,        // "_id"
            16, 0, 0, 0,            // int "16" (length)
            4,                      // type (B_UUID_STANDARD)
            2, 0, 0, 0, 0, 0, 0, 0, //
            1, 0, 0, 0, 0, 0, 0, 0, // 8 bytes for long, 2 longs for UUID
            0];                     // EOM
        BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(wrap(binaryTypeWithUUIDAsBytes))), true);
        Binary binary;
        try {
            reader.readStartDocument();
            binary = reader.readBinaryData();
        } finally {
            reader.close();
        }

        when:
        UUID actualUUID = binaryToUUIDTransformer.transform(binary);

        then:
        actualUUID == new UUID(2L, 1L);
    }
}
