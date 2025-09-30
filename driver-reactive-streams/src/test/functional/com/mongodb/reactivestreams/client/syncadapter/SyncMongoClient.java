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

import com.mongodb.ClientBulkWriteException;
import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.assertions.Assertions;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCluster;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.internal.BatchCursor;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

public class SyncMongoClient implements MongoClient {

    private static boolean waitForBatchCursorCreation;

    /**
     * Unfortunately this is the only way to wait for a query to be initiated, since Reactive Streams is asynchronous
     * and we have no way of knowing. Tests which require cursor initiation to complete before execution of the next operation
     * can set this to a positive value.  A value of 256 ms has been shown to work well. The default value is 0.
     */
    public static void enableSleepAfterCursorOpen(final long sleepMS) {
        SyncMongoCluster.enableSleepAfterCursorOpen(sleepMS);
    }

    /**
     * Unfortunately this is the only way to wait for error logic to complete, since it's asynchronous.
     * This is inherently racy but there are not any other good options. Tests which require cursor error handling to complete before
     * execution of the next operation can set this to a positive value.  A value of 256 ms has been shown to work well. The default
     * value is 0.
     */
    public static void enableSleepAfterCursorError(final long sleepMS) {
        SyncMongoCluster.enableSleepAfterCursorError(sleepMS);
    }

    /**
     * Unfortunately this is the only way to wait for close to complete, since it's asynchronous.
     * This is inherently racy but there are not any other good options. Tests which require cursor cancellation to complete before
     * execution of the next operation can set this to a positive value.  A value of 256 ms has been shown to work well. The default
     * value is 0.
     */
    public static void enableSleepAfterCursorClose(final long sleepMS) {
        SyncMongoCluster.enableSleepAfterCursorClose(sleepMS);
    }

    /**
     * Enables {@linkplain Thread#sleep(long) sleeping} in {@link SyncClientSession#close()} to wait until asynchronous closing actions
     * are done. It is an attempt to make asynchronous {@link SyncMongoClient#close()} method synchronous;
     * the attempt is racy and incorrect, but good enough for tests given that no other approach is available.
     */
    public static void enableSleepAfterSessionClose(final long sleepMS) {
        SyncMongoCluster.enableSleepAfterSessionClose(sleepMS);
    }

    /**
     * Enables behavior for waiting until a reactive {@link BatchCursor} is created.
     * <p>
     * When enabled, {@link SyncMongoCursor} allows intercepting the result of the cursor creation process.
     * If the creation fails, the resulting exception will be propagated; if successful, the
     * process will proceed to issue getMore commands.
     * <p>
     * NOTE:  Do not enable when multiple cursors are being iterated concurrently.
     */
    public static void enableWaitForBatchCursorCreation() {
        waitForBatchCursorCreation = true;
    }

    public static boolean isWaitForBatchCursorCreationEnabled() {
        return waitForBatchCursorCreation;
    }

    public static void disableWaitForBatchCursorCreation() {
        waitForBatchCursorCreation = false;
    }

    public static void disableSleep() {
        SyncMongoCluster.disableSleep();
    }

    public static long getSleepAfterCursorOpen() {
        return SyncMongoCluster.getSleepAfterCursorOpen();
    }

    public static long getSleepAfterCursorError() {
        return SyncMongoCluster.getSleepAfterCursorError();
    }

    public static long getSleepAfterCursorClose() {
        return SyncMongoCluster.getSleepAfterCursorClose();
    }

    public static long getSleepAfterSessionClose() {
        return SyncMongoCluster.getSleepAfterSessionClose();
    }

    private final com.mongodb.reactivestreams.client.MongoClient wrapped;
    private final SyncMongoCluster delegate;
    private final ConnectionPoolCounter connectionPoolCounter;

    public SyncMongoClient(final MongoClientSettings settings) {
        this(settings, null);
    }

    public SyncMongoClient(final MongoClientSettings settings, @Nullable final MongoDriverInformation mongoDriverInformation) {
        this(MongoClientSettings.builder(settings), mongoDriverInformation);
    }

    public SyncMongoClient(final MongoClientSettings.Builder builder) {
        this(builder, null);
    }

    public SyncMongoClient(final MongoClientSettings.Builder builder, @Nullable final MongoDriverInformation mongoDriverInformation) {
        this.connectionPoolCounter = new ConnectionPoolCounter();
        builder.applyToConnectionPoolSettings(b -> b.addConnectionPoolListener(connectionPoolCounter));
        this.wrapped = MongoClients.create(builder.build(), mongoDriverInformation);
        this.delegate = new SyncMongoCluster(wrapped);
    }

    public com.mongodb.reactivestreams.client.MongoClient getWrapped() {
        return wrapped;
    }

    @Override
    public CodecRegistry getCodecRegistry() {
        return delegate.getCodecRegistry();
    }

    @Override
    public ReadPreference getReadPreference() {
        return delegate.getReadPreference();
    }

    @Override
    public WriteConcern getWriteConcern() {
        return delegate.getWriteConcern();
    }

    @Override
    public ReadConcern getReadConcern() {
        return delegate.getReadConcern();
    }

    @Override
    public Long getTimeout(final TimeUnit timeUnit) {
        return delegate.getTimeout(timeUnit);
    }

    @Override
    public MongoCluster withCodecRegistry(final CodecRegistry codecRegistry) {
        return delegate.withCodecRegistry(codecRegistry);
    }

    @Override
    public MongoCluster withReadPreference(final ReadPreference readPreference) {
        return delegate.withReadPreference(readPreference);
    }

    @Override
    public MongoCluster withWriteConcern(final WriteConcern writeConcern) {
        return delegate.withWriteConcern(writeConcern);
    }

    @Override
    public MongoCluster withReadConcern(final ReadConcern readConcern) {
        return delegate.withReadConcern(readConcern);
    }

    @Override
    public MongoCluster withTimeout(final long timeout, final TimeUnit timeUnit) {
        return delegate.withTimeout(timeout, timeUnit);
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return delegate.getDatabase(databaseName);
    }

    @Override
    public ClientSession startSession() {
        return delegate.startSession();
    }

    @Override
    public ClientSession startSession(final ClientSessionOptions options) {
        return delegate.startSession(options);
    }

    @Override
    public MongoIterable<String> listDatabaseNames() {
        return delegate.listDatabaseNames();
    }

    @Override
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        return delegate.listDatabaseNames(clientSession);
    }


    @Override
    public ListDatabasesIterable<Document> listDatabases() {
        return delegate.listDatabases();
    }

    @Override
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return delegate.listDatabases(clientSession);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final Class<TResult> resultClass) {
        return delegate.listDatabases(resultClass);
    }

    @Override
    public <TResult> ListDatabasesIterable<TResult> listDatabases(final ClientSession clientSession, final Class<TResult> resultClass) {
        return delegate.listDatabases(clientSession, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch() {
        return delegate.watch();
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return delegate.watch(resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return delegate.watch(pipeline);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return delegate.watch(pipeline, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return delegate.watch(clientSession);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return delegate.watch(clientSession, resultClass);
    }

    @Override
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return delegate.watch(clientSession, pipeline);
    }

    @Override
    public <TResult> ChangeStreamIterable<TResult> watch(
            final ClientSession clientSession, final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return delegate.watch(clientSession, pipeline, resultClass);
    }

    @Override
    public void close() {
        wrapped.close();
       int activeConnections = connectionPoolCounter.getActiveConnections();
       Assertions.assertTrue(activeConnections == 0,
               format("Expected all connections to be closed after closing the client. %n"
                       + "The connection pool listener reports '%d' open connections.", activeConnections));
    }


    @Override
    public ClientBulkWriteResult bulkWrite(
            final List<? extends ClientNamespacedWriteModel> clientWriteModels) throws ClientBulkWriteException {
        return delegate.bulkWrite(clientWriteModels);
    }

    @Override
    public ClientBulkWriteResult bulkWrite(
            final List<? extends ClientNamespacedWriteModel> clientWriteModels,
            final ClientBulkWriteOptions options) throws ClientBulkWriteException {
        return delegate.bulkWrite(clientWriteModels, options);
    }

    @Override
    public ClientBulkWriteResult bulkWrite(
            final ClientSession clientSession,
            final List<? extends ClientNamespacedWriteModel> clientWriteModels) throws ClientBulkWriteException {
        return delegate.bulkWrite(clientSession, clientWriteModels);
    }

    @Override
    public ClientBulkWriteResult bulkWrite(
            final ClientSession clientSession,
            final List<? extends ClientNamespacedWriteModel> clientWriteModels,
            final ClientBulkWriteOptions options) throws ClientBulkWriteException {
        return delegate.bulkWrite(clientSession, clientWriteModels, options);
    }

    @Override
    public ClusterDescription getClusterDescription() {
        return wrapped.getClusterDescription();
    }

    @Override
    public void appendMetadata(final MongoDriverInformation mongoDriverInformation) {
        wrapped.appendMetadata(mongoDriverInformation);
    }

    static class ConnectionPoolCounter implements ConnectionPoolListener {
        private final AtomicInteger activeConnections = new AtomicInteger(0);

        @Override
        public void connectionCheckedOut(final ConnectionCheckedOutEvent event) {
            activeConnections.incrementAndGet();
        }

        @Override
        public void connectionCheckedIn(final ConnectionCheckedInEvent event) {
            activeConnections.decrementAndGet();
        }

        public int getActiveConnections() {
            return activeConnections.get();
        }
    }
}
