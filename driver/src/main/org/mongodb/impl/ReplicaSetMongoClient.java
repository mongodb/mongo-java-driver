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

import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoOperations;
import org.mongodb.ServerAddress;
import org.mongodb.async.MongoAsyncOperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A {@code MongoClient} implementation that is capable of providing a logical connection to a replica set.
 */
class ReplicaSetMongoClient extends AbstractMongoClient {
    private final List<ServerAddress> seedList;
//    private final ReplicaSetMonitor replicaSetMonitor;
    private Map<ServerAddress, SingleServerMongoClient> mongoClientMap = new HashMap<ServerAddress, SingleServerMongoClient>();

    ReplicaSetMongoClient(final List<ServerAddress> seedList, final MongoClientOptions options) {
        super(options);
        this.seedList = Collections.unmodifiableList(new ArrayList<ServerAddress>(seedList));
//        replicaSetMonitor = new ReplicaSetMonitor(seedList, this);
//        replicaSetMonitor.start();
    }

    @Override
    public MongoOperations getOperations() {
        return null;
 //       return new ReplicaSetMongoOperations(this);
    }

    @Override
    public MongoAsyncOperations getAsyncOperations() {
        return null;
//        return new ReplicaSetMongoOperations(this);
    }

    @Override
    public void withConnection(final Runnable runnable) {
//        getPrimary().withConnection(runnable);
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        return null;
//        return getPrimary().withConnection(callable);
    }

    @Override
    public void close() {
//        replicaSetMonitor.close();
        for (MongoClient cur : mongoClientMap.values()) {
            cur.close();
        }
    }

    @Override
    void bindToConnection() {
//        getPrimary().bindToConnection();
    }

    @Override
    void unbindFromConnection() {
 //       getPrimary().bindToConnection();
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return seedList;
    }

}
