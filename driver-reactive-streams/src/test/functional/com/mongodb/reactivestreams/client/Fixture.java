/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoTimeoutException;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerVersion;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Helper class for asynchronous tests.
 */
public final class Fixture {
    private static MongoClient mongoClient;
    private static ServerVersion serverVersion;
    private static ClusterType clusterType;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(ClusterFixture.getConnectionString());
            serverVersion = getServerVersion();
            clusterType = getClusterType();
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static MongoDatabase getDefaultDatabase() {
        return getMongoClient().getDatabase(getDefaultDatabaseName());
    }

    public static MongoCollection<Document> initializeCollection(final MongoNamespace namespace) throws Throwable {
        MongoDatabase database = getMongoClient().getDatabase(namespace.getDatabaseName());
        try {
            ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();
            database.runCommand(new Document("drop", namespace.getCollectionName())).subscribe(subscriber);
            subscriber.await(10, SECONDS);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().startsWith("ns not found")) {
                throw e;
            }
        }
        return database.getCollection(namespace.getCollectionName());
    }

    public static void dropDatabase(final String name) throws Throwable {
        if (name == null) {
            return;
        }
        try {
            ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();
            getMongoClient().getDatabase(name).runCommand(new Document("dropDatabase", 1)).subscribe(subscriber);
            subscriber.await(10, SECONDS);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().startsWith("ns not found")) {
                throw e;
            }
        }
    }

    public static void drop(final MongoNamespace namespace) throws Throwable {
        try {
            ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();
            getMongoClient().getDatabase(namespace.getDatabaseName())
                    .runCommand(new Document("drop", namespace.getCollectionName()))
                    .subscribe(subscriber);
            subscriber.await(20, SECONDS);
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    public static boolean serverVersionAtLeast(final int majorVersion, final int minorVersion) {
        getMongoClient();
        return serverVersion.compareTo(new ServerVersion(Arrays.asList(majorVersion, minorVersion, 0))) >= 0;
    }

    public static boolean isReplicaSet() {
        getMongoClient();
        return clusterType == ClusterType.REPLICA_SET;
    }

    @SuppressWarnings("unchecked")
    private static ServerVersion getServerVersion() {
        Document response = runAdminCommand(new Document("buildInfo", 1));
        List<Integer> versionArray = (List<Integer>) response.get("versionArray");
        return new ServerVersion(versionArray.subList(0, 3));
    }

    private static ClusterType getClusterType() {
        Document response = runAdminCommand(new Document("ismaster", 1));
        if (response.containsKey("setName")) {
            return ClusterType.REPLICA_SET;
        } else if ("isdbgrid".equals(response.getString("msg"))) {
            return ClusterType.SHARDED;
        } else {
            return ClusterType.STANDALONE;
        }
    }

    private static Document runAdminCommand(final Bson command) {
        ObservableSubscriber<Document> subscriber = new ObservableSubscriber<Document>();
        getMongoClient().getDatabase("admin")
                .runCommand(command)
                .subscribe(subscriber);
        try {
            return subscriber.get(20, SECONDS).get(0);
        } catch (Throwable t) {
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new RuntimeException(t);
        }
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                dropDatabase(com.mongodb.client.Fixture.getDefaultDatabaseName());
            } catch (Throwable e) {
                // ignore
            }
            mongoClient.close();
            mongoClient = null;
        }
    }

    public static class ObservableSubscriber<T> implements Subscriber<T> {
        private final List<T> received;
        private final List<Throwable> errors;
        private final CountDownLatch latch;
        private volatile Subscription subscription;
        private volatile boolean completed;

        public ObservableSubscriber() {
            this.received = new ArrayList<T>();
            this.errors = new ArrayList<Throwable>();
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void onSubscribe(final Subscription s) {
            subscription = s;
        }

        @Override
        public void onNext(final T t) {
            received.add(t);
        }

        @Override
        public void onError(final Throwable t) {
            errors.add(t);
            onComplete();
        }

        @Override
        public void onComplete() {
            completed = true;
            latch.countDown();
        }

        public Subscription getSubscription() {
            return subscription;
        }

        public List<T> getReceived() {
            return received;
        }

        public List<Throwable> getErrors() {
            return errors;
        }

        public boolean isCompleted() {
            return completed;
        }

        public List<T> get(final long timeout, final TimeUnit unit) throws Throwable {
            return await(timeout, unit).getReceived();
        }

        public ObservableSubscriber<T> await(final long timeout, final TimeUnit unit) throws Throwable {
            subscription.request(Integer.MAX_VALUE);
            if (!latch.await(timeout, unit)) {
                throw new MongoTimeoutException("Publisher onComplete timed out");
            }
            if (!errors.isEmpty()) {
                throw errors.get(0);
            }
            return this;
        }
    }

    public static class CountingSubscriber<T> implements Subscriber<T> {
        private int counter = 0;
        private Throwable error;
        private final CountDownLatch latch = new CountDownLatch(1);
        private volatile Subscription subscription;
        private volatile boolean completed;

        @Override
        public void onSubscribe(final Subscription s) {
            subscription = s;
        }

        @Override
        public void onNext(final T t) {
            counter += 1;
        }

        @Override
        public void onError(final Throwable t) {
            error = t;
            onComplete();
        }

        @Override
        public void onComplete() {
            completed = true;
            latch.countDown();
        }

        public Subscription getSubscription() {
            return subscription;
        }

        public int getCount() {
            return counter;
        }

        public Throwable getError() {
            return error;
        }

        public boolean isCompleted() {
            return completed;
        }

        public int get(final long timeout, final TimeUnit unit) throws Throwable {
            subscription.request(Integer.MAX_VALUE);
            return await(timeout, unit).getCount();
        }

        public CountingSubscriber<T> await(final long timeout, final TimeUnit unit) throws Throwable {
            if (!latch.await(timeout, unit)) {
                if (!isCompleted()) {
                    subscription.cancel();
                }
                throw new MongoTimeoutException("Publisher onComplete timed out");
            }
            if (!isCompleted()) {
                subscription.cancel();
            }
            if (error != null) {
                throw error;
            }
            return this;
        }
    }

}
