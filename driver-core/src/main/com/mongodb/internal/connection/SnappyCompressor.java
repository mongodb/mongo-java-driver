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
import com.mongodb.internal.connection.netty.NettyByteBuf;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

class SnappyCompressor extends Compressor {
    @Override
    public String getName() {
        return "snappy";
    }

    @Override
    public byte getId() {
        return 1;
    }

    // the server does not support the framing format so SnappyFramedOutputStream can't be used.  The entire source message must first
    // be copied into a single byte array.  For that reason the compress method defined in the base class can't be used.
    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        int uncompressedSize = getUncompressedSize(source);

        byte[] singleByteArraySource = new byte[uncompressedSize];
        copy(source, singleByteArraySource);

        try {
            byte[] out = new byte[Snappy.maxCompressedLength(uncompressedSize)];
            int compressedSize = Snappy.compress(singleByteArraySource, 0, singleByteArraySource.length, out, 0);
            target.writeBytes(out, 0, compressedSize);
        } catch (IOException e) {
            throw new MongoInternalException("Unexpected IOException", e);
        }
    }

    private int getUncompressedSize(final List<ByteBuf> source) {
        int uncompressedSize = 0;
        for (ByteBuf cur : source) {
            uncompressedSize += cur.remaining();
        }
        return uncompressedSize;
    }

    private void copy(final List<ByteBuf> source, final byte[] in) {
        int offset = 0;
        for (ByteBuf cur : source) {
            int remaining = cur.remaining();
            cur.get(in, offset, remaining);
            offset += remaining;
        }
    }

    @Override
    void uncompress(final ByteBuf source, final ByteBuf target) {
        int expected = target.remaining();
        int written;
        try {
            if (source.isBackedByArray() && target.isBackedByArray()) {
                byte[] input = source.array();
                int offset = source.arrayOffset() + source.position();
                int length = source.remaining();
                checkSize(Snappy.uncompressedLength(input, offset, length), expected);
                written = Snappy.uncompress(input, offset, length, target.array(), target.arrayOffset() + target.position());
            } else {
                // A Netty composite buffer can materialize a detached copy when asked for one NIO view.
                boolean composite = target instanceof NettyByteBuf && ((NettyByteBuf) target).asByteBuf().nioBufferCount() != 1;
                ByteBuffer output = composite ? null : target.asNIO();
                ByteBuffer input = source.asNIO();
                if (output == null || !output.isDirect() || output.isReadOnly() || !input.isDirect()) {
                    super.uncompress(source, target);
                    checkSize(expected - target.remaining(), expected);
                    return;
                }
                checkSize(Snappy.uncompressedLength(input), expected);
                // Xerial updates the destination limit. Isolate those changes from the driver's buffer state.
                written = Snappy.uncompress(input.duplicate(), output.duplicate());
            }
        } catch (IOException e) {
            throw new MongoInternalException("Unable to decompress Snappy response", e);
        }
        checkSize(written, expected);
        source.position(source.limit());
        target.position(target.position() + written);
    }

    private static void checkSize(final int actual, final int expected) {
        if (actual != expected) {
            throw new MongoInternalException("Expected a " + expected + "-byte Snappy response, got " + actual + " bytes");
        }
    }

    @Override
    InputStream getInputStream(final InputStream source) throws IOException {
        return new SnappyInputStream(source);
    }
}
