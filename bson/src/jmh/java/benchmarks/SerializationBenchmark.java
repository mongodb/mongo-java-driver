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

package benchmarks;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.bson.BsonBinaryWriter;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Mine
 * ----------------------------------------------------------------------------------
 Benchmark                                                       Mode  Cnt         Score         Error   Units
 SerializationBenchmark.toByteArray_large                       thrpt   10         1.920 ±       0.709  ops/ms
 SerializationBenchmark.toByteArray_large:gc.alloc.rate.norm    thrpt   10  13_388_928.163 ±       0.107    B/op
 SerializationBenchmark.toByteArray_small                       thrpt   10    209_157.248 ±    8860.801  ops/ms
 SerializationBenchmark.toByteArray_small:gc.alloc.rate.norm    thrpt   10        80.000 ±       0.001    B/op
 SerializationBenchmark.write_id                                thrpt   10     33_845.743 ±    3292.592  ops/ms
 SerializationBenchmark.write_id:gc.alloc.rate.norm             thrpt   10        80.000 ±       0.001    B/op
 SerializationBenchmark.write_largeDocument                     thrpt   10         0.024 ±       0.001  ops/ms
 SerializationBenchmark.write_largeDocument:gc.alloc.rate.norm  thrpt   10  33_120_450.944 ± 5621183.081    B/op
 SerializationBenchmark.write_numbers                           thrpt   10     16_249.730 ±     801.250  ops/ms
 SerializationBenchmark.write_numbers:gc.alloc.rate.norm        thrpt   10        80.000 ±       0.001    B/op

 *
 * Master ----------------------------------------------------------------------------
 *
 * Benchmark                                                       Mode  Cnt         Score      Error   Units
 * SerializationBenchmark.toByteArray_large                       thrpt   10         0.887 ±    0.073  ops/ms
 * SerializationBenchmark.toByteArray_large:gc.alloc.rate.norm    thrpt   10  26_777_870.800 ±   15.285    B/op
 * SerializationBenchmark.toByteArray_small                       thrpt   10    108_887.377 ± 3914.072  ops/ms
 * SerializationBenchmark.toByteArray_small:gc.alloc.rate.norm    thrpt   10       160.000 ±    0.001    B/op
 * SerializationBenchmark.write_id                                thrpt   10     24_058.792 ± 1726.869  ops/ms
 * SerializationBenchmark.write_id:gc.alloc.rate.norm             thrpt   10       112.000 ±    0.001    B/op
 * SerializationBenchmark.write_largeDocument                     thrpt   10         0.032 ±    0.013  ops/ms
 * SerializationBenchmark.write_largeDocument:gc.alloc.rate.norm  thrpt   10  38_400_448.921 ±    3.630    B/op
 * SerializationBenchmark.write_numbers                           thrpt   10     13_154.730 ±  167.654  ops/ms
 * SerializationBenchmark.write_numbers:gc.alloc.rate.norm        thrpt   10        80.000 ±    0.001    B/op
 * -----------------------------------
 */
@SuppressWarnings("all")
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SerializationBenchmark {

  @State(Scope.Thread)
  public static class Input {

    public OutputBuffer buffer, smallBuffer, largeBuffer;
    public BsonBinaryWriter writer;
    public int num;
    public double d;
    public ObjectId id;

    @Setup
    public void setup() {
      this.buffer = new BasicOutputBuffer();
      this.largeBuffer = new BasicOutputBuffer();
      this.smallBuffer = new BasicOutputBuffer();

      this.num = ThreadLocalRandom.current().nextInt();
      this.d = ThreadLocalRandom.current().nextDouble();
      this.id = new ObjectId();

      this.writer = new BsonBinaryWriter(this.buffer);
      this.writer.writeStartDocument();
      this.writer.mark();

      try (BsonBinaryWriter writer = new BsonBinaryWriter(this.largeBuffer)) {
        writer.writeStartDocument();
        writer.writeStartArray("array");
        for (int i = 0; i < 300_000; ++i) {
          writeDoc(writer);
        }
        writer.writeEndArray();
        writer.writeEndDocument();
      }

      try (BsonBinaryWriter writer = new BsonBinaryWriter(this.smallBuffer)) {
        writer.writeStartDocument();
        writer.writeStartArray("array");
        writeDoc(writer);
        writer.writeEndArray();
        writer.writeEndDocument();
      }
    }

    private void writeDoc(BsonBinaryWriter writer) {
      writer.writeStartDocument();
      writer.writeObjectId("_id", this.id);
      writer.writeDouble("value", ThreadLocalRandom.current().nextDouble());
      writer.writeEndDocument();
    }
  }

  @Benchmark
  public void write_numbers(Input input, Blackhole blackhole) {
    BsonBinaryWriter writer = input.writer;
    writer.writeInt32("a", input.num);
    writer.writeInt64("b", input.num);
    writer.writeDouble("c", input.d);
    writer.writeDateTime("d", input.num);
    writer.reset();
    writer.mark();
    blackhole.consume(input.buffer);
  }

  @Benchmark
  public void write_id(Input input, Blackhole blackhole) {
    BsonBinaryWriter writer = input.writer;
    writer.writeObjectId("a", input.id);
    writer.reset();
    writer.mark();
    blackhole.consume(input.buffer);
  }

  @Benchmark
  public void toByteArray_large(Input input, Blackhole blackhole) {
    blackhole.consume(input.largeBuffer.toByteArray());
  }

  @Benchmark
  public void toByteArray_small(Input input, Blackhole blackhole) {
    blackhole.consume(input.smallBuffer.toByteArray());
  }

  @Benchmark
  public void write_largeDocument(Input input, Blackhole blackhole) {
    try (BsonBinaryWriter writer = new BsonBinaryWriter(input.largeBuffer)) {
      writer.writeStartDocument();
      writer.writeStartArray("a");
      for (int i = 0; i < 300_000; ++i) {
        input.writeDoc(writer);
      }
      writer.writeEndArray();
      writer.writeEndDocument();
    }
    blackhole.consume(input.largeBuffer);
    input.largeBuffer.truncateToPosition(0);
  }
}
