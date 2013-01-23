/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.impl;

import org.bson.util.BufferPool;
import org.mongodb.ClientAdmin;
import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.io.PowerOfTwoByteBufferPool;

import java.nio.ByteBuffer;

public abstract class AbstractMongoClient implements MongoClient {
    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool;
    private final ClientAdmin admin;

    public AbstractMongoClient(final MongoClientOptions options) {
        this(options, new PowerOfTwoByteBufferPool(24));
    }

    public AbstractMongoClient(final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool) {
        this.options = options;
        this.bufferPool = bufferPool;
        admin = new ClientAdminImpl(getOperations(), options.getPrimitiveSerializers());
    }

    @Override
    public MongoDatabaseImpl getDatabase(final String databaseName) {
        return getDatabase(databaseName, MongoDatabaseOptions.builder().build());
    }

    @Override
    public MongoDatabaseImpl getDatabase(final String databaseName, final MongoDatabaseOptions optionsForOperation) {
        return new MongoDatabaseImpl(databaseName, this, optionsForOperation.withDefaults(this.getOptions()));
    }

    @Override
    public MongoClientOptions getOptions() {
        return options;
    }

    @Override
    public ClientAdmin admin() {
        return admin;
    }

    BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }
}
