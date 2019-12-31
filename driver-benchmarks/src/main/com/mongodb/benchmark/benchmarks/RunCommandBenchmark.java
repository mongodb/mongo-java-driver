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

package com.mongodb.benchmark.benchmarks;

import com.mongodb.client.MongoDatabase;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.conversions.Bson;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonReader;

public class RunCommandBenchmark<T extends Bson> extends AbstractMongoBenchmark {

    private MongoDatabase database;
    private final Codec<T> codec;
    private final T command;

    public RunCommandBenchmark(final Codec<T> codec) {
        this.codec = codec;
        this.command = createCommand();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        database = client.getDatabase("admin");
    }

    @Override
    public String getName() {
        return "Run command";
    }

    @Override
    public void run() {
        for (int i = 0; i < NUM_INTERNAL_ITERATIONS; i++) {
            database.runCommand(command);
        }
    }

    @Override
    public int getBytesPerRun() {
        return NUM_INTERNAL_ITERATIONS * getCommandSize();
    }

    private int getCommandSize() {
        T command = createCommand();
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        codec.encode(new BsonBinaryWriter(buffer), command, EncoderContext.builder().build());
        return buffer.getSize();
    }

    private T createCommand() {
        return codec.decode(new JsonReader("{ismaster: true}"),DecoderContext.builder().build());
    }
}
