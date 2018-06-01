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

import org.bson.BsonBinaryReader;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;

import java.nio.ByteBuffer;

public class BsonDecodingBenchmark<T> extends AbstractBsonDocumentBenchmark<T> {

    public BsonDecodingBenchmark(final String name, final String resourcePath, final Codec<T> codec) {
        super(name + " BSON Decoding", resourcePath, codec);
    }

    @Override
    public void run() {
        for (int i = 0; i < NUM_INTERNAL_ITERATIONS; i++) {
           codec.decode(new BsonBinaryReader(ByteBuffer.wrap(documentBytes)), DecoderContext.builder().build());
        }
    }
}
