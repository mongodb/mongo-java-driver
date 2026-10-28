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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.mongodb.MongoInternalException;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.util.List;

class ZstdCompressor extends Compressor {
    private static final int MAX_RETAINED_BUFFER_SIZE = 1024 * 1024;
    private static final CompressionContextPool<CompressionContext> COMPRESSION_CONTEXTS =
            new CompressionContextPool<>(CompressionContext::new, CompressionContext::close);
    private static final CompressionContextPool<ZstdDecompressCtx> DECOMPRESSION_CONTEXTS =
            new CompressionContextPool<>(ZstdDecompressCtx::new, ZstdDecompressCtx::close);

    @Override
    public String getName() {
        return "zstd";
    }

    @Override
    public byte getId() {
        return 3;
    }

    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        int uncompressedSize = getUncompressedSize(source);
        COMPRESSION_CONTEXTS.use(context -> {
            try {
                byte[] in = context.uncompressedBuffer(uncompressedSize);
                copy(source, in);
                byte[] out = context.compressedBuffer((int) Zstd.compressBound(uncompressedSize));
                int compressedSize = context.ctx.compressByteArray(out, 0, out.length, in, 0, uncompressedSize);
                target.writeBytes(out, 0, compressedSize);
            } catch (Exception e) {
                throw new MongoInternalException("Unexpected exception", e);
            }
        });
    }

    @Override
    void uncompress(final ByteBuf source, final ByteBuf target) {
        DECOMPRESSION_CONTEXTS.use(ctx -> uncompressFully(source, target, (in, inOffset, inLength, out, outOffset, outLength) ->
                ctx.decompressByteArray(out, outOffset, outLength, in, inOffset, inLength)));
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

    private static final class CompressionContext {
        private final ZstdCompressCtx ctx = new ZstdCompressCtx().setLevel(Zstd.defaultCompressionLevel());
        private byte[] uncompressed = new byte[0];
        private byte[] compressed = new byte[0];

        byte[] uncompressedBuffer(final int size) {
            if (size > MAX_RETAINED_BUFFER_SIZE) {
                return new byte[size];
            }
            if (uncompressed.length < size) {
                uncompressed = new byte[size];
            }
            return uncompressed;
        }

        byte[] compressedBuffer(final int size) {
            if (size > MAX_RETAINED_BUFFER_SIZE) {
                return new byte[size];
            }
            if (compressed.length < size) {
                compressed = new byte[size];
            }
            return compressed;
        }

        void close() {
            ctx.close();
        }
    }
}
