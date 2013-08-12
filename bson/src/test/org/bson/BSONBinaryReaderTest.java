package org.bson;

import org.bson.io.BasicInputBuffer;
import org.bson.types.DBPointer;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BSONBinaryReaderTest {

    private BSONBinaryReader reader;

    @Test
    public void testReadDBPointer() {
        reader = createReaderForBytes(new byte[]{
                26, 0, 0, 0, 12, 97, 0, 2, 0, 0, 0, 98, 0, 82, 9, 41, 108,
                -42, -60, -29, -116, -7, 111, -1, -36, 0
        });

        reader.readStartDocument();
        assertThat(reader.readBSONType(), is(BSONType.DB_POINTER));
        final DBPointer dbPointer = reader.readDBPointer();
        assertThat(dbPointer.getNamespace(), is("b"));
        assertThat(dbPointer.getId(), is(new ObjectId("5209296cd6c4e38cf96fffdc")));
        reader.readEndDocument();
    }

    private BSONBinaryReader createReaderForBytes(final byte[] bytes) {
        return new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(ByteBuffer.wrap(bytes))), true);
    }
}
