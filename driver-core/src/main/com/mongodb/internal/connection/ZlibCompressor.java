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

import com.mongodb.MongoCompressor;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

class ZlibCompressor extends Compressor {
    private static final int CHUNK_SIZE = 16 * 1024;
    private static final byte[] EMPTY = new byte[0];
    private static final ConcurrentHashMap<Integer, CompressionContextPool<CompressionContext>> COMPRESSION_CONTEXTS =
            new ConcurrentHashMap<>();
    private static final CompressionContextPool<Inflater> INFLATERS = new CompressionContextPool<>(Inflater::new, Inflater::end);

    private final CompressionContextPool<CompressionContext> compressionContexts;

    ZlibCompressor(final MongoCompressor mongoCompressor) {
        int level = mongoCompressor.getPropertyNonNull(MongoCompressor.LEVEL, Deflater.DEFAULT_COMPRESSION);
        this.compressionContexts = COMPRESSION_CONTEXTS.computeIfAbsent(level, l ->
                new CompressionContextPool<>(() -> new CompressionContext(l), CompressionContext::end));
    }

    @Override
    public String getName() {
        return "zlib";
    }

    @Override
    public byte getId() {
        return 2;
    }

    @Override
    void compress(final List<ByteBuf> source, final BsonOutput target) {
        CompressionContext context = compressionContexts.get();
        boolean reusable = false;
        try {
            Deflater deflater = context.deflater;
            deflater.reset();
            for (ByteBuf cur : source) {
                while (cur.hasRemaining()) {
                    if (cur.isBackedByArray()) {
                        int length = cur.remaining();
                        deflater.setInput(cur.array(), cur.arrayOffset() + cur.position(), length);
                        cur.position(cur.limit());
                    } else {
                        int length = Math.min(cur.remaining(), context.input.length);
                        cur.get(context.input, 0, length);
                        deflater.setInput(context.input, 0, length);
                    }
                    while (!deflater.needsInput()) {
                        target.writeBytes(context.output, 0, deflater.deflate(context.output));
                    }
                }
            }
            deflater.finish();
            while (!deflater.finished()) {
                target.writeBytes(context.output, 0, deflater.deflate(context.output));
            }
            deflater.setInput(EMPTY);
            reusable = true;
        } finally {
            compressionContexts.release(context, reusable);
        }
    }

    @Override
    void uncompress(final ByteBuf source, final ByteBuf target) {
        Inflater inflater = INFLATERS.get();
        boolean reusable = false;
        try {
            uncompressFully(source, target, (in, inOffset, inLength, out, outOffset, outLength) ->
                    inflate(inflater, in, inOffset, inLength, out, outOffset, outLength));
            reusable = true;
        } finally {
            INFLATERS.release(inflater, reusable);
        }
    }

    private static int inflate(final Inflater inflater, final byte[] in, final int inOffset, final int inLength,
            final byte[] out, final int outOffset, final int outLength) throws DataFormatException {
        inflater.reset();
        inflater.setInput(in, inOffset, inLength);
        int inflated = inflater.inflate(out, outOffset, outLength);
        if (!inflater.finished()) {
            throw new DataFormatException("The zlib stream does not end within the uncompressed size of the message");
        }
        inflater.setInput(EMPTY);
        return inflated;
    }

    private static final class CompressionContext {
        private final Deflater deflater;
        private final byte[] input = new byte[CHUNK_SIZE];
        private final byte[] output = new byte[CHUNK_SIZE];

        CompressionContext(final int level) {
            deflater = new Deflater(level);
        }

        void end() {
            deflater.end();
        }
    }
}
