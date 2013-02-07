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
 */

package org.mongodb.impl;

import org.bson.util.BufferPool;
import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoClientURI;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.async.AsyncDetector;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for MongoClient instances.
 *
 * @since 3.0
 */
@ThreadSafe
public final class MongoClientsImpl {
    private MongoClientsImpl() {
    }

    public static SingleServerMongoClient create(final ServerAddress serverAddress, final MongoClientOptions options) {
        if (AsyncDetector.javaVersionSupportsAsync()) {
            return new SingleServerAsyncMongoClient(serverAddress, options);
        }
        else {
            return new SingleServerSyncMongoClient(serverAddress, options);
        }
    }

    public static SingleServerMongoClient create(final ServerAddress serverAddress, final MongoClientOptions options,
                                                 final BufferPool<ByteBuffer> bufferPool) {
        if (AsyncDetector.javaVersionSupportsAsync()) {
            return new SingleServerAsyncMongoClient(serverAddress, options, bufferPool);
        }
        else {
            return new SingleServerSyncMongoClient(serverAddress, options, bufferPool);
        }
    }

    public static MongoClient create(final List<ServerAddress> seedList, final MongoClientOptions options) {
        return new MongoClientImpl(options, new ReplicaSetMongoOperations(seedList, options));
    }

    public static MongoClient create(final MongoClientURI mongoURI) throws UnknownHostException {
        return create(mongoURI, mongoURI.getOptions());
    }

    public static MongoClient create(final MongoClientURI mongoURI, final MongoClientOptions options) throws UnknownHostException {
        if (mongoURI.getHosts().size() == 1) {
            return create(new ServerAddress(mongoURI.getHosts().get(0)), options);
        }
        else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>();
            for (String cur : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(cur));
            }
            return create(seedList, options);
        }
    }
}