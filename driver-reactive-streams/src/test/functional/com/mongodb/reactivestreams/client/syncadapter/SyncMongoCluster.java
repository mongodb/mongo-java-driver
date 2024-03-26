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
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.reactivestreams.client.syncadapter.ContextHelper.CONTEXT;
import static java.util.Objects.requireNonNull;

public class SyncMongoCluster implements MongoCluster {

    private static long sleepAfterCursorOpenMS;
    private static long sleepAfterCursorErrorMS;
    private static long sleepAfterCursorCloseMS;
    private static long sleepAfterSessionCloseMS;

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
            throw new IllegalArgumentException("sleepMS must be a positive value");
        }
        sleepAfterCursorOpenMS = sleepMS;
    }

    /**
     * Unfortunately this is the only way to wait for error logic to complete, since it's asynchronous.
     * This is inherently racy but there are not any other good options. Tests which require cursor error handling to complete before
     * execution of the next operation can set this to a positive value.  A value of 256 ms has been shown to work well. The default
     * value is 0.
     */
    public static void enableSleepAfterCursorError(final long sleepMS) {
        if (sleepAfterCursorErrorMS != 0) {
            throw new IllegalStateException("Already enabled");
        }
        if (sleepMS <= 0) {
            throw new IllegalArgumentException("sleepMS must be a positive value");
        }
        sleepAfterCursorErrorMS = sleepMS;
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
            throw new IllegalArgumentException("sleepMS must be a positive value");
        }
        sleepAfterCursorCloseMS = sleepMS;
    }


    /**
     * Enables {@linkplain Thread#sleep(long) sleeping} in {@link SyncClientSession#close()} to wait until asynchronous closing actions
     * are done. It is an attempt to make asynchronous {@link SyncMongoClient#close()} method synchronous;
     * the attempt is racy and incorrect, but good enough for tests given that no other approach is available.
     */
    public static void enableSleepAfterSessionClose(final long sleepMS) {
        if (sleepAfterSessionCloseMS != 0) {
            throw new IllegalStateException("Already enabled");
        }
        if (sleepMS <= 0) {
            throw new IllegalArgumentException("sleepMS must be a positive value");
        }
        sleepAfterSessionCloseMS = sleepMS;
    }

    public static void disableSleep() {
        sleepAfterCursorOpenMS = 0;
        sleepAfterCursorErrorMS = 0;
        sleepAfterCursorCloseMS = 0;
        sleepAfterSessionCloseMS = 0;
    }

    public static long getSleepAfterCursorOpen() {
        return sleepAfterCursorOpenMS;
    }

    public static long getSleepAfterCursorError() {
        return sleepAfterCursorErrorMS;
    }

    public static long getSleepAfterCursorClose() {
        return sleepAfterCursorCloseMS;
    }

    public static long getSleepAfterSessionClose() {
        return sleepAfterSessionCloseMS;
    }

    private final com.mongodb.reactivestreams.client.MongoCluster wrapped;

    public SyncMongoCluster(final com.mongodb.reactivestreams.client.MongoCluster wrapped) {
        this.wrapped = wrapped;
    }

    public com.mongodb.reactivestreams.client.MongoCluster getWrapped() {
        return wrapped;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return wrapped.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return wrapped.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return wrapped.getReadConcern();
    }

    @Override
    public Long getTimeout(final TimeUnit timeUnit) {
        return wrapped.getTimeout(timeUnit);
    }

    @Override
    public MongoCluster withCodecRegistry(final CodecRegistry codecRegistry) {
        return new SyncMongoCluster(wrapped.withCodecRegistry(codecRegistry));
    }

    @Override
    public MongoCluster withReadPreference(final ReadPreference readPreference) {
        return new SyncMongoCluster(wrapped.withReadPreference(readPreference));
    }

    @Override
    public MongoCluster withWriteConcern(final WriteConcern writeConcern) {
        return new SyncMongoCluster(wrapped.withWriteConcern(writeConcern));
    }

    @Override
    public MongoCluster withReadConcern(final ReadConcern readConcern) {
        return new SyncMongoCluster(wrapped.withReadConcern(readConcern));
    }

    @Override
    public MongoCluster withTimeout(final long timeout, final TimeUnit timeUnit) {
        return new SyncMongoCluster(wrapped.withTimeout(timeout, timeUnit));
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return new SyncMongoDatabase(wrapped.getDatabase(databaseName));
    }

    @Override
    public ClientSession startSession() {
        return new SyncClientSession(requireNonNull(Mono.from(wrapped.startSession()).contextWrite(CONTEXT).block(TIMEOUT_DURATION)), this);
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
        return new SyncClientSession(requireNonNull(Mono.from(wrapped.startSession(options)).contextWrite(CONTEXT).block(TIMEOUT_DURATION)), this);
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return listDatabases(BsonDocument.class).nameOnly(true).map(result -> result.getString("name").getValue());
    }

    @Override
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        return listDatabases(clientSession, BsonDocument.class).nameOnly(true).map(result -> result.getString("name").getValue());
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return new SyncListDatabasesIterable<>(wrapped.listDatabases());
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final Class<TResult> resultClass) {
        return new SyncListDatabasesIterable<>(wrapped.listDatabases(resultClass));
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final ClientSession clientSession, final Class<TResult> resultClass) {
        return new SyncListDatabasesIterable<>(wrapped.listDatabases(unwrap(clientSession), resultClass));
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

    private com.mongodb.reactivestreams.client.ClientSession unwrap(final ClientSession clientSession) {
        return ((SyncClientSession) clientSession).getWrapped();
    }
}
