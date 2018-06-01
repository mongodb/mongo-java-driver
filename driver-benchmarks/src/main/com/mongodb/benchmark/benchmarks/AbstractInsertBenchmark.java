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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.json.JsonReader;

import java.nio.charset.StandardCharsets;

public abstract class AbstractInsertBenchmark<T> extends AbstractMongoBenchmark {

    protected MongoCollection<T> collection;

    private final String name;
    private final String resourcePath;
    private final Class<T> clazz;
    private byte[] bytes;
    protected int fileLength;
    protected T document;

    protected AbstractInsertBenchmark(final String name, final String resourcePath, final Class<T> clazz) {
        this.name = name;
        this.resourcePath = resourcePath;
        this.clazz = clazz;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MongoDatabase database = client.getDatabase(DATABASE_NAME);

        collection = database.getCollection(COLLECTION_NAME, clazz);

        database.drop();
        bytes = readAllBytesFromRelativePath(resourcePath);

        fileLength = bytes.length;

        Codec<T> codec = collection.getCodecRegistry().get(clazz);

        document = codec.decode(new JsonReader(new String(bytes, StandardCharsets.UTF_8)), DecoderContext.builder().build());
    }

    @Override
    public void before() throws Exception {
        super.before();
        collection.drop();
    }

    @Override
    public String getName() {
        return name;
    }

    protected T createDocument() {
        Codec<T> codec = collection.getCodecRegistry().get(clazz);

        return codec.decode(new JsonReader(new String(bytes, StandardCharsets.UTF_8)), DecoderContext.builder().build());
    }
}
