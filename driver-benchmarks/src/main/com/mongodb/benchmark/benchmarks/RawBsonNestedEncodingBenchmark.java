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

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;

import java.io.IOException;

public class RawBsonNestedEncodingBenchmark extends BsonEncodingBenchmark<BsonDocument> {

    public RawBsonNestedEncodingBenchmark(final String name, final String resourcePath) {
        super(name, resourcePath, new BsonDocumentCodec());
    }

    @Override
    public void setUp() throws IOException {
        super.setUp();

        RawBsonDocument rawDoc = new RawBsonDocument(document, codec);
        document = new BsonDocument("nested", rawDoc);

        documentBytes = getDocumentAsBuffer(document);
    }

    @Override
    public int getBytesPerRun() {
        return documentBytes.length * NUM_INTERNAL_ITERATIONS;
    }
}