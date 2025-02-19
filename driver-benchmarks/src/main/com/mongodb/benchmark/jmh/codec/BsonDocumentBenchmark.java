package com.mongodb.benchmark.jmh.codec;

import com.mongodb.internal.connection.ByteBufferBsonOutput;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 2, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
public class BsonDocumentBenchmark {

    @State(Scope.Benchmark)
    public static class Input {
        protected final PowerOfTwoBufferPool bufferPool = PowerOfTwoBufferPool.DEFAULT;
        protected final Codec<BsonDocument> codec = getDefaultCodecRegistry().get(BsonDocument.class);
        protected BsonDocument document;
        protected byte[] documentBytes;

        @Setup
        public void setup() throws IOException {

            BsonArray bsonValues = new BsonArray();
            for (int i = 0; i < 1000; i++) {
                bsonValues.add(new BsonDouble(i));
            }

            document = new BsonDocument("array", bsonValues);
            documentBytes = getDocumentAsBuffer(document);
        }

        private byte[] getDocumentAsBuffer(final BsonDocument document) throws IOException {
            BasicOutputBuffer buffer = new BasicOutputBuffer();
            codec.encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());

            ByteArrayOutputStream baos = new ByteArrayOutputStream(buffer.getSize());
            buffer.pipe(baos);
            return baos.toByteArray();
        }
    }

    @Benchmark
    public void decode(@NotNull Input input, @NotNull Blackhole blackhole) {
        blackhole.consume(input.codec.decode(new BsonBinaryReader(ByteBuffer.wrap(input.documentBytes)), DecoderContext.builder().build()));
    }

    @Benchmark
    public void encode(@NotNull Input input, @NotNull Blackhole blackhole) {
        input.codec.encode(new BsonBinaryWriter(new ByteBufferBsonOutput(input.bufferPool)), input.document, EncoderContext.builder().build());
        blackhole.consume(input);
    }
}
