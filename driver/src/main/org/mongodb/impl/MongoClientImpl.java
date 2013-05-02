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

import org.mongodb.ClientAdmin;
import org.mongodb.Datastore;
import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoConnector;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.MongoSession;
import org.mongodb.ServerAddress;
import org.mongodb.codecs.PrimitiveCodecs;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class MongoClientImpl implements MongoClient {

    private final MongoConnector connector;
    private final MongoClientOptions clientOptions;
    private PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();
    private final ThreadLocal<MongoSession> pinnedSession = new ThreadLocal<MongoSession>();

    public MongoClientImpl(final MongoClientOptions clientOptions, final MongoConnector connector) {
        this.clientOptions = clientOptions;
        this.connector = connector;
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return getDatabase(databaseName, MongoDatabaseOptions.builder().build());
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName, final MongoDatabaseOptions options) {
        return new MongoDatabaseImpl(databaseName, this, options.withDefaults(clientOptions));
    }

    @Override
    public void withConnection(final Runnable runnable) {
        pinConnection();
        try {
            runnable.run();
        } finally {
            pinnedSession.remove();
        }
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        pinConnection();
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally {
            pinnedSession.remove();
        }
    }

    @Override
    public void close() {
        connector.close();
    }

    @Override
    public MongoClientOptions getOptions() {
        return clientOptions;
    }

    @Override
    public ClientAdmin tools() {
        return new ClientAdminImpl(connector, primitiveCodecs);
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return connector.getServerAddressList();
    }

    @Override
    public Datastore getDatastore(final String databaseName) {
        return new PojoDatastore(getDatabase(databaseName));
    }

    public MongoConnector getConnector() {
        return connector;
    }

    public MongoSession getSession() {
        if (pinnedSession.get() != null) {
            return pinnedSession.get();
        }
        return connector;
    }

    private void pinConnection() {
        if (pinnedSession.get() != null) {
            throw new IllegalStateException();
        }
        pinnedSession.set(connector.getSession());
    }
}
