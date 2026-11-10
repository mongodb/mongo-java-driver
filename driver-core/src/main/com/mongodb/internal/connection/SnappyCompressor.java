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
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.List;

class SnappyCompressor extends Compressor {
    private static final CompressionContextPool<Buffers> BUFFERS = new CompressionContextPool<>(Buffers::new, buffers -> { });

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
        BUFFERS.use(buffers -> {
            try {
                byte[] singleByteArraySource = buffers.uncompressed.atLeast(uncompressedSize);
                copy(source, singleByteArraySource);
                byte[] out = buffers.compressed.atLeast(Snappy.maxCompressedLength(uncompressedSize));
                int compressedSize = Snappy.compress(singleByteArraySource, 0, uncompressedSize, out, 0);
                target.writeBytes(out, 0, compressedSize);
            } catch (IOException e) {
                throw new MongoInternalException("Unexpected IOException", e);
            }
        });
    }

    @Override
    void uncompress(final ByteBuf source, final ByteBuf target) {
        uncompressFully(source, target, (in, inOffset, inLength, out, outOffset, outLength) -> {
            int uncompressedLength = Snappy.uncompressedLength(in, inOffset, inLength);
            if (uncompressedLength != outLength) {
                return uncompressedLength;
            }
            return Snappy.uncompress(in, inOffset, inLength, out, outOffset);
        });
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

    private static final class Buffers {
        private final ReusableArray uncompressed = new ReusableArray();
        private final ReusableArray compressed = new ReusableArray();
    }
}
