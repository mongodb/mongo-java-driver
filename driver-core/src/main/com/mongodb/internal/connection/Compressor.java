/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;

abstract class Compressor {

    static final int BUFFER_SIZE = 256;

    abstract String getName();

    abstract byte getId();

    void compress(final List<ByteBuf> source, final BsonOutput target) {
        BufferExposingByteArrayOutputStream baos = new BufferExposingByteArrayOutputStream(1024);
        try (OutputStream outputStream = getOutputStream(baos)) {
            byte[] scratch = new byte[BUFFER_SIZE];
            for (ByteBuf cur : source) {
                while (cur.hasRemaining()) {
                    int numBytes = Math.min(cur.remaining(), scratch.length);
                    cur.get(scratch, 0, numBytes);
                    outputStream.write(scratch, 0, numBytes);
                }
            }

        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        }
        // ignore
        target.writeBytes(baos.getInternalBytes(), 0, baos.size());
    }

    void uncompress(final ByteBuf source, final ByteBuf target) {

        try (InputStream inputStream = getInputStream(new ByteBufInputStream(source))) {
            byte[] scratch = new byte[BUFFER_SIZE];
            int numBytes = inputStream.read(scratch);
            while (numBytes != -1) {
                target.put(scratch, 0, numBytes);
                numBytes = inputStream.read(scratch);
            }
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        }
        // ignore
    }

    /**
     * Decompresses all of {@code source} into {@code target} in one call. The remaining space in {@code target} must be exactly the
     * uncompressed size.
     */
    static void uncompressFully(final ByteBuf source, final ByteBuf target, final ArrayDecompressor decompressor) {
        int sourceLength = source.remaining();
        int targetLength = target.remaining();
        byte[] sourceArray;
        int sourceOffset;
        if (source.isBackedByArray()) {
            sourceArray = source.array();
            sourceOffset = source.arrayOffset() + source.position();
        } else {
            sourceArray = new byte[sourceLength];
            source.get(sourceArray);
            sourceOffset = 0;
        }
        boolean targetBackedByArray = target.isBackedByArray();
        byte[] targetArray = targetBackedByArray ? target.array() : new byte[targetLength];
        int targetOffset = targetBackedByArray ? target.arrayOffset() + target.position() : 0;

        int uncompressedLength;
        try {
            uncompressedLength = decompressor.decompress(sourceArray, sourceOffset, sourceLength, targetArray, targetOffset, targetLength);
        } catch (Exception e) {
            throw new MongoInternalException("Unexpected exception", e);
        }
        if (uncompressedLength != targetLength) {
            throw new MongoInternalException("The uncompressed size of the message does not match its header: expected "
                    + targetLength + " bytes");
        }

        if (source.isBackedByArray()) {
            source.position(source.limit());
        }
        if (targetBackedByArray) {
            target.position(target.position() + targetLength);
        } else {
            target.put(targetArray, 0, targetLength);
        }
    }

    interface ArrayDecompressor {
        int decompress(byte[] source, int sourceOffset, int sourceLength, byte[] target, int targetOffset, int targetLength)
                throws Exception;
    }

    static final class ReusableArray {
        private static final int MAX_RETAINED_SIZE = 1024 * 1024;
        private byte[] array = new byte[0];

        byte[] atLeast(final int size) {
            if (size > MAX_RETAINED_SIZE) {
                return new byte[size];
            }
            if (array.length < size) {
                array = new byte[size];
            }
            return array;
        }
    }

    // override this if not overriding the compress method
    OutputStream getOutputStream(final OutputStream source) throws IOException {
        throw new UnsupportedEncodingException();
    }

    // override this if not overriding the uncompress method
    InputStream getInputStream(final InputStream source) throws IOException {
        throw new UnsupportedOperationException();
    }

    private static final class ByteBufInputStream extends InputStream {
        private final ByteBuf source;

        ByteBufInputStream(final ByteBuf source) {
            this.source = source;
        }

        @Override
        public int read(final byte[] bytes, final int offset, final int length) {
            if (!source.hasRemaining()) {
                return -1;
            }

            int bytesToRead = length > source.remaining() ? source.remaining() : length;
            source.get(bytes, offset, bytesToRead);
            return bytesToRead;
        }

        @Override
        public int read() {
            throw new UnsupportedOperationException();
        }
    }

    // Just so we don't have to copy the buffer
    private static final class BufferExposingByteArrayOutputStream extends ByteArrayOutputStream {
        BufferExposingByteArrayOutputStream(final int size) {
            super(size);
        }

        byte[] getInternalBytes() {
            return buf;
        }
    }
}
