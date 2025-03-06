/*
 * Copyright 2016-present MongoDB, Inc.
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

package com.mongodb.benchmark.jmh.codec;

import com.mongodb.internal.connection.ByteBufferBsonOutput;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import com.mongodb.lang.NonNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import static com.mongodb.benchmark.jmh.codec.BsonUtils.getDocumentAsBuffer;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
public class BsonArrayCodecBenchmark {

    @State(Scope.Benchmark)
    public static class Input {
        protected final PowerOfTwoBufferPool bufferPool = PowerOfTwoBufferPool.DEFAULT;
        protected final BsonArrayCodec bsonArrayCodec = new BsonArrayCodec();
        protected BsonDocument document;
        protected byte[] documentBytes;
        private BsonBinaryReader reader;
        private BsonBinaryWriter writer;
        private BsonArray bsonValues;

        @Setup
        public void setup() throws IOException {
            bsonValues = new BsonArray();
            document = new BsonDocument("array", bsonValues);

            for (int i = 0; i < 1000; i++) {
                bsonValues.add(new BsonDouble(i));
            }

            documentBytes = getDocumentAsBuffer(document);
        }

        @Setup(Level.Invocation)
        public void beforeIteration() {
            reader = new BsonBinaryReader(ByteBuffer.wrap(documentBytes));
            writer = new BsonBinaryWriter(new ByteBufferBsonOutput(bufferPool));

            reader.readStartDocument();
            writer.writeStartDocument();
            writer.writeName("array");
        }
    }

    @Benchmark
    public void decode(@NonNull Input input, @NonNull Blackhole blackhole) {
        blackhole.consume(input.bsonArrayCodec.decode(input.reader, DecoderContext.builder().build()));
    }

    @Benchmark
    public void encode(@NonNull Input input, @NonNull Blackhole blackhole) {
        input.bsonArrayCodec.encode(input.writer, input.bsonValues, EncoderContext.builder().build());
        blackhole.consume(input);
    }
}
