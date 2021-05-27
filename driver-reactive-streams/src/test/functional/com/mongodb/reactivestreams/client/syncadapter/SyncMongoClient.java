/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.syncadapter;

import com.mongodb.ClientSessionOptions;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.ClusterDescription;
import org.bson.Document;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static java.util.Objects.requireNonNull;

public class SyncMongoClient implements MongoClient {

    private static long sleepAfterCursorOpenMS;

    private static long sleepAfterCursorCloseMS;

    /**
     * Unfortunately this is the only way to wait for a query to be initiated, since Reactive Streams is asynchronous
     * and we have no way of knowing. Tests which require cursor initiation to complete before execution of the next operation
     * can set this to a positive value.  A value of 256 ms has been shown to work well. The default value is 0.
     */
    public static void enableSleepAfterCursorOpen(final long sleepMS) {
        if (sleepAfterCursorOpenMS != 0) {
            throw new IllegalStateException("Already enabled");
        }
        if (sleepMS <= 0) {
            throw new IllegalArgumentException("sleepMS must be a postive value");
        }
        sleepAfterCursorOpenMS = sleepMS;
    }

    /**
     * Unfortunately this is the only way to wait for close to complete, since it's asynchronous.
     * This is inherently racy but there are not any other good options. Tests which require cursor cancellation to complete before
     * execution of the next operation can set this to a positive value.  A value of 256 ms has been shown to work well. The default
     * value is 0.
     */
    public static void enableSleepAfterCursorClose(final long sleepMS) {
        if (sleepAfterCursorCloseMS != 0) {
            throw new IllegalStateException("Already enabled");
        }
        if (sleepMS <= 0) {
            throw new IllegalArgumentException("sleepMS must be a postive value");
        }
        sleepAfterCursorCloseMS = sleepMS;
    }

    public static void disableCursorSleep() {
        sleepAfterCursorOpenMS = 0;
        sleepAfterCursorCloseMS = 0;
    }

    public static long getSleepAfterCursorOpen() {
        return sleepAfterCursorOpenMS;
    }

    public static long getSleepAfterCursorClose() {
        return sleepAfterCursorCloseMS;
    }

    private final com.mongodb.reactivestreams.client.MongoClient wrapped;

    public SyncMongoClient(final com.mongodb.reactivestreams.client.MongoClient wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return new SyncMongoDatabase(wrapped.getDatabase(databaseName));
    }

    @Override
    public ClientSession startSession() {
        return new SyncClientSession(requireNonNull(Mono.from(wrapped.startSession()).block(TIMEOUT_DURATION)), this);
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
        return new SyncClientSession(requireNonNull(Mono.from(wrapped.startSession(options)).block(TIMEOUT_DURATION)), this);
    }

    @Override
    public void close() {
        wrapped.close();
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        throw new UnsupportedOperationException();
    }

    @Override
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return new SyncListDatabasesIterable<Document>(wrapped.listDatabases());
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final Class<TResult> resultClass) {
        return new SyncListDatabasesIterable<TResult>(wrapped.listDatabases(resultClass));
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final ClientSession clientSession, final Class<TResult> resultClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return new SyncChangeStreamIterable<>(wrapped.watch());
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(resultClass));
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return new SyncChangeStreamIterable<>(wrapped.watch(pipeline));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(pipeline, resultClass));
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession)));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), resultClass));
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), pipeline));
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        return new SyncChangeStreamIterable<>(wrapped.watch(unwrap(clientSession), pipeline, resultClass));
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return wrapped.getClusterDescription();
    }

    private com.mongodb.reactivestreams.client.ClientSession unwrap(final ClientSession clientSession) {
        return ((SyncClientSession) clientSession).getWrapped();
    }
}
