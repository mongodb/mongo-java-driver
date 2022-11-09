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
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoTimeoutException;
import org.bson.Document;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Helper class for asynchronous tests.
 */
public final class MongoFixture {
    private static MongoClient mongoClient;

    private MongoFixture() {
    }

    public static final long DEFAULT_TIMEOUT_MILLIS = 5000L;
    public static final long PUBLISHER_REFERENCE_CLEANUP_TIMEOUT_MILLIS = 1000L;

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            mongoClient = MongoClients.create(getMongoClientSettings());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static MongoClientSettings getMongoClientSettings() {
        return getMongoClientSettingsBuilder().build();
    }

    public static MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        return MongoClientSettings.builder().applyConnectionString(ClusterFixture.getConnectionString());
    }

    public static String getDefaultDatabaseName() {
        return ClusterFixture.getDefaultDatabaseName();
    }

    public static MongoDatabase getDefaultDatabase() {
        return getMongoClient().getDatabase(getDefaultDatabaseName());
    }

    public static void dropDatabase(final String name) {
        if (name == null) {
            return;
        }
        try {
            run(getMongoClient().getDatabase(name).runCommand(new Document("dropDatabase", 1)));
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    public static void drop(final MongoNamespace namespace) {
        try {
            run(getMongoClient().getDatabase(namespace.getDatabaseName()).runCommand(new Document("drop", namespace.getCollectionName())));
        } catch (MongoCommandException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    public static <T> List<T> run(final Publisher<T> publisher) {
        return run(publisher, () -> {});
    }

    public static <T> List<T> run(final Publisher<T> publisher, final Runnable onRequest) {
        try {
            ObservableSubscriber<T> subscriber = new ObservableSubscriber<>(onRequest);
            publisher.subscribe(subscriber);
            return subscriber.get();
        } catch (Throwable t) {
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            }
            throw new RuntimeException(t);
        }
    }

    public static void cleanDatabases() {
        List<String> dbNames = MongoFixture.run(getMongoClient().listDatabaseNames());
        for (String dbName : dbNames) {
            if (dbName.startsWith(getDefaultDatabaseName())) {
                dropDatabase(dbName);
            }
        }
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            cleanDatabases();
            mongoClient.close();
            mongoClient = null;
        }
    }

    public static class ObservableSubscriber<T> implements Subscriber<T> {
        private final List<T> received;
        private final List<Throwable> errors;
        private final CountDownLatch latch;
        private final Runnable onRequest;
        private volatile boolean requested;
        private volatile Subscription subscription;
        private volatile boolean completed;

        public ObservableSubscriber() {
            this(() -> {});
        }

        public ObservableSubscriber(final Runnable onRequest) {
            this.received = new ArrayList<>();
            this.errors = new ArrayList<>();
            this.latch = new CountDownLatch(1);
            this.onRequest = onRequest;
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

        public List<T> get() {
            return await(60, SECONDS).getReceived();
        }

        public List<T> get(final long timeout, final TimeUnit unit) {
            return await(timeout, unit).getReceived();
        }

        public ObservableSubscriber<T> await(final long timeout, final TimeUnit unit) {
            return await(Integer.MAX_VALUE, timeout, unit);
        }

        public ObservableSubscriber<T> await(final int request, final long timeout, final TimeUnit unit) {
            subscription.request(request);
            if (!requested) {
                requested = true;
                onRequest.run();
            }
            try {
                if (!latch.await(timeout, unit)) {
                    throw new MongoTimeoutException("Publisher onComplete timed out");
                }
            } catch (InterruptedException e) {
                throw new MongoException("Await failed", e);
            }
            if (!errors.isEmpty()) {
                throw new MongoException("Await failed", errors.get(0));
            }
            return this;
        }
    }

}
