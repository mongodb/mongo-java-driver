/*
 * Copyright 2017 MongoDB, Inc.
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
 *
 */

package com.mongodb.connection;

import com.mongodb.MongoCompressor;
import com.mongodb.MongoInternalException;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

class ZlibCompressor implements Compressor {

    private static final int BUFFER_SIZE = 256;

    private final int level;

    ZlibCompressor(final MongoCompressor mongoCompressor) {
        this.level = mongoCompressor.getProperty(MongoCompressor.LEVEL, Deflater.DEFAULT_COMPRESSION);
    }

    @Override
    public String getName() {
        return "zlib";
    }

    /**
     * Gets the compression level
     *
     * @return the compression level
     */
    public int getLevel() {
        return level;
    }

    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        BufferExposingByteArrayOutputStream baos = new BufferExposingByteArrayOutputStream(1024);
        Deflater deflater = new Deflater(level);
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(baos, deflater);
        try {
            byte[] scratch = new byte[BUFFER_SIZE];
            for (ByteBuf cur : source) {
                while (cur.hasRemaining()) {
                    int numBytes = Math.min(cur.remaining(), scratch.length);
                    cur.get(scratch, 0, numBytes);
                    deflaterOutputStream.write(scratch, 0, numBytes);
                }
            }

            deflaterOutputStream.finish();
            target.writeBytes(baos.getInternalBytes(), 0, baos.size());
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        } finally {
            try {
                deflaterOutputStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }


    @Override
    public void uncompress(final ByteBuf source, final ByteBuf target) {
        InputStream is = new ByteBufInputStream(source);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(is);

        try {
            byte[] scratch = new byte[256];
            int numBytes = inflaterInputStream.read(scratch);
            while (numBytes != -1) {
                target.put(scratch, 0, numBytes);
                numBytes = inflaterInputStream.read(scratch);
            }
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        } finally {
            try {
                inflaterInputStream.close();
            } catch (IOException e) {
                // ignore
            }
        }

    }

    @Override
    public byte getId() {
        return 2;
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

    private static final class ByteBufInputStream extends InputStream {
        private final ByteBuf source;

        private ByteBufInputStream(final ByteBuf source) {
            this.source = source;
        }

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
}
