/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.benchmark.framework;

import org.bson.json.JsonMode;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class EvergreenBenchmarkResultWriter implements BenchmarkResultWriter {

    private static final String OUTPUT_FILE_SYSTEM_PROPERTY = "org.mongodb.benchmarks.output";

    private final Writer writer;
    private final JsonWriter jsonWriter;

    public EvergreenBenchmarkResultWriter()  {
        try {
            writer = new FileWriter(System.getProperty(OUTPUT_FILE_SYSTEM_PROPERTY));
            jsonWriter = new JsonWriter(writer,
                    JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).indent(true).build());;
            jsonWriter.writeStartDocument();
            jsonWriter.writeStartArray("results");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(final BenchmarkResult benchmarkResult) {
        jsonWriter.writeStartDocument();
        jsonWriter.writeString("name", benchmarkResult.getName());
        jsonWriter.writeStartDocument("results");
        jsonWriter.writeStartDocument("1");

        jsonWriter.writeDouble("ops_per_sec",
                (benchmarkResult.getBytesPerIteration() / 1000000d) /
                        (benchmarkResult.getElapsedTimeNanosAtPercentile(50) / 1000000000d));

        jsonWriter.writeEndDocument();
        jsonWriter.writeEndDocument();
        jsonWriter.writeEndDocument();
    }

    @Override
    public void close() throws IOException {
        jsonWriter.writeEndArray();
        jsonWriter.writeEndDocument();
        jsonWriter.close();
        writer.close();
        System.out.println("DONE");
    }

    public String getResults() {
        return writer.toString();
    }
}
