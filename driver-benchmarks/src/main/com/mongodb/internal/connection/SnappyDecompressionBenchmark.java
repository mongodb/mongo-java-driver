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

import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Random;

/** Decodes one raw wire payload into a caller-owned response buffer. */
@State(Scope.Thread)
public class SnappyDecompressionBenchmark {
    @Param({"256", "512", "1024", "4096", "8192", "16384", "32768", "65536", "131072", "1048576"})
    private int size;
    @Param({"heap", "direct"})
    private String layout;
    @Param({"structured", "binary"})
    private String content;

    private final SnappyCompressor compressor = new SnappyCompressor();
    private ByteBuf input;
    private ByteBuf output;

    @Setup
    public void setup() throws IOException {
        byte[] data = new byte[size];
        new Random(47).nextBytes(data);
        if (content.equals("structured")) {
            for (int i = 0; i < size; i++) {
                if (i % 64 < 48) {
                    data[i] = (byte) (i % 13);
                }
            }
        }
        byte[] encoded = Snappy.compress(data);
        ByteBuffer src = allocate(encoded.length);
        src.put(encoded);
        ((Buffer) src).flip();
        input = new ByteBufNIO(src);
        output = new ByteBufNIO(allocate(size));
    }

    private ByteBuffer allocate(final int length) {
        return layout.equals("heap") ? ByteBuffer.allocate(length) : ByteBuffer.allocateDirect(length);
    }

    @Benchmark
    public int decode() {
        input.position(0);
        output.position(0);
        compressor.uncompress(input, output);
        return output.position();
    }

    @TearDown
    public void close() {
        input.release();
        output.release();
    }
}
