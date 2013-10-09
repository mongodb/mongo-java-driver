/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;

import org.bson.BSONBinaryReader;
import org.bson.BSONBinaryWriter;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A simple wrapper around a byte array that is the representation of a single BSON document.
 */
// TODO: Add an easy way to iterate over the fields?
public class BSONDocumentBuffer {
    private final byte[] bytes;

    /**
     * Constructs a new instance with the given byte array.  Note that it does not make a copy of the array, so do not modify it after
     * passing it to this constructor.
     *
     * @param bytes the bytes representing a BSON document.  Note that the byte array is NOT copied, so care must be taken not to modify it
     *              after passing it to this construction, unless of course that is your intention.
     */
    public BSONDocumentBuffer(final byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes can not be null");
        }
        this.bytes = bytes;
    }

    /**
     * Construct a new instance from the given document and codec for the document type.
     *
     * @param document the document to transform
     * @param codec    the codec to facilitate the transformation
     */
    public <T> BSONDocumentBuffer(final T document, final Codec<T> codec) {
        BSONBinaryWriter writer = new BSONBinaryWriter(new BasicOutputBuffer(), true);
        try {
            codec.encode(writer, document);
            this.bytes = writer.getBuffer().toByteArray();
        } finally {
            writer.close();
        }
    }

    /**
     * Returns a {@code ByteBuf} that wraps the byte array, with the proper byte order.  Any changes made to the returned will be reflected
     * in the underlying byte array owned by this instance.
     *
     * @return a byte buffer that wraps the byte array owned by this instance.
     */
    public ByteBuf getByteBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return new ByteBufNIO(buffer);
    }

    /**
     * Decode this into a document.
     *
     * @param codec the codec to facilitate the transformation
     * @return the decoded document
     */
    public <T> T decode(final Codec<T> codec) {
        BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(getByteBuffer()), true);
        try {
            return codec.decode(reader);
        } finally {
            reader.close();
        }
    }
}
