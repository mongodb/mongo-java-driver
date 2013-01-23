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

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoOperations;
import org.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

class ReplicaSetMongoClient extends AbstractMongoClient {
    private final List<ServerAddress> seedList;
    private final SingleServerMongoClient primary;

    public ReplicaSetMongoClient(final List<ServerAddress> seedList, final MongoClientOptions options) {
        super(options);
        this.seedList = Collections.unmodifiableList(new ArrayList<ServerAddress>(seedList));
        primary = new SingleServerMongoClient(seedList.get(0), options);
    }

    @Override
    public MongoOperations getOperations() {
        return primary.getOperations();
    }

    @Override
    public void withConnection(final Runnable runnable) {
        primary.withConnection(runnable);
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        return primary.withConnection(callable);
    }

    @Override
    public void close() {
        primary.close();
    }

    @Override
    void bindToConnection() {
        primary.bindToConnection();
    }

    @Override
    void unbindFromConnection() {
        primary.bindToConnection();
    }

    @Override
    List<ServerAddress> getServerAddressList() {
        return seedList;
    }
}
