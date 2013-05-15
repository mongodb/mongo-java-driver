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

package org.mongodb;

import org.mongodb.async.AsyncSession;
import org.mongodb.impl.DefaultAsyncConnectionFactory;
import org.mongodb.impl.DefaultConnectionFactory;
import org.mongodb.impl.MongoClientImpl;
import org.mongodb.impl.MongoClientsImpl;
import org.mongodb.io.BufferPool;
import org.mongodb.io.PowerOfTwoByteBufferPool;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Helper class for the acceptance tests.  Considering replacing with MongoClientTestBase.
 */
public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";

    private static MongoClientURI mongoClientURI;
    private static MongoClientImpl mongoClient;
    private static BufferPool<ByteBuffer> bufferPool = new PowerOfTwoByteBufferPool();
    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            final MongoClientURI mongoURI = getMongoClientURI();
            try {
                mongoClient = MongoClientsImpl.create(mongoURI);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid Mongo URI: " + mongoURI.getURI(), e);
            }
        }
        return mongoClient;
    }

    public static synchronized MongoClientURI getMongoClientURI() {
        if (mongoClientURI == null) {
            final String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
            final String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty()
                    ? DEFAULT_URI : mongoURIProperty;
            mongoClientURI = new MongoClientURI(mongoURIString);
        }
        return mongoClientURI;
    }

    public static Cluster getCluster() {
        getMongoClient();
        return mongoClient.getCluster();
    }

    public static Session getSession() {
        getMongoClient();
        return mongoClient.getSession();
    }

    public static AsyncSession getAsyncSession() {
        getMongoClient();
        return mongoClient.getAsyncSession();
    }

    // Note this is not safe for concurrent access - if you run multiple tests in parallel from the same class,
    // you'll drop the DB
    public static MongoDatabase getCleanDatabaseForTest(final Class<?> testClass) {
        final MongoDatabase database = getMongoClient().getDatabase(testClass.getSimpleName());

        database.tools().drop();
        return database;
    }

    public static BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    public static MongoClientOptions getOptions() {
        return getMongoClientURI().getOptions();
    }

    public static ServerAddress getPrimary() throws UnknownHostException {
        return new ServerAddress(getMongoClientURI().getHosts().get(0));
    }

    public static List<MongoCredential> getCredentialList() {
        return getMongoClientURI().getCredentials();
    }

    public static ConnectionFactory getConnectionFactory() throws UnknownHostException {
        return new DefaultConnectionFactory(getOptions(), getPrimary(), getBufferPool(), getCredentialList());
    }

    public static AsyncConnectionFactory getAsyncConnectionFactory() throws UnknownHostException {
        return new DefaultAsyncConnectionFactory(getOptions(), getPrimary(), getBufferPool(), getCredentialList());
    }
}
