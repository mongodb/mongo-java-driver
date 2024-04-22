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

package com.mongodb.client;

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The client-side representation of a MongoDB cluster operations.
 *
 * <p>
 * The originating {@link MongoClient} is responsible for the closing of resources.
 * If the originator {@link MongoClient} is closed, then any cluster operations will fail.
 * </p>
 *
 * @see MongoClient
 * @since CSOT
 */
@Immutable
public interface MongoCluster {

    /**
     * Get the codec registry for the MongoCluster.
     *
     * @return the {@link org.bson.codecs.configuration.CodecRegistry}
     * @since CSOT
     */
    CodecRegistry getCodecRegistry();

    /**
     * Get the read preference for the MongoCluster.
     *
     * @return the {@link com.mongodb.ReadPreference}
     * @since CSOT
     */
    ReadPreference getReadPreference();

    /**
     * Get the write concern for the MongoCluster.
     *
     * @return the {@link com.mongodb.WriteConcern}
     * @since CSOT
     */
    WriteConcern getWriteConcern();

    /**
     * Get the read concern for the MongoCluster.
     *
     * @return the {@link com.mongodb.ReadConcern}
     * @since CSOT
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    ReadConcern getReadConcern();

    /**
     * The time limit for the full execution of an operation.
     *
     * <p>If not null the following deprecated options will be ignored:
     * {@code waitQueueTimeoutMS}, {@code socketTimeoutMS}, {@code wTimeoutMS}, {@code maxTimeMS} and {@code maxCommitTimeMS}</p>
     *
     * <ul>
     *   <li>{@code null} means that the timeout mechanism for operations will defer to using:
     *    <ul>
     *        <li>{@code waitQueueTimeoutMS}: The maximum wait time in milliseconds that a thread may wait for a connection to become
     *        available</li>
     *        <li>{@code socketTimeoutMS}: How long a send or receive on a socket can take before timing out.</li>
     *        <li>{@code wTimeoutMS}: How long the server will wait for the write concern to be fulfilled before timing out.</li>
     *        <li>{@code maxTimeMS}: The cumulative time limit for processing operations on a cursor.
     *        See: <a href="https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS">cursor.maxTimeMS</a>.</li>
     *        <li>{@code maxCommitTimeMS}: The maximum amount of time to allow a single {@code commitTransaction} command to execute.
     *        See: {@link com.mongodb.TransactionOptions#getMaxCommitTime}.</li>
     *   </ul>
     *   </li>
     *   <li>{@code 0} means infinite timeout.</li>
     *    <li>{@code > 0} The time limit to use for the full execution of an operation.</li>
     * </ul>
     *
     * @param timeUnit the time unit
     * @return the timeout in the given time unit
     * @since CSOT
     */
    @Alpha(Alpha.Reason.CLIENT)
    @Nullable
    Long getTimeout(TimeUnit timeUnit);

    /**
     * Create a new MongoCluster instance with a different codec registry.
     *
     * <p>The {@link CodecRegistry} configured by this method is effectively treated by the driver as an instance of
     * {@link org.bson.codecs.configuration.CodecProvider}, which {@link CodecRegistry} extends. So there is no benefit to defining
     * a class that implements {@link CodecRegistry}. Rather, an application should always create {@link CodecRegistry} instances
     * using the factory methods in {@link org.bson.codecs.configuration.CodecRegistries}.</p>
     *
     * @param codecRegistry the new {@link org.bson.codecs.configuration.CodecRegistry} for the database
     * @return a new MongoCluster instance with the different codec registry
     * @see org.bson.codecs.configuration.CodecRegistries
     * @since CSOT
     */
    MongoCluster withCodecRegistry(CodecRegistry codecRegistry);

    /**
     * Create a new MongoCluster instance with a different read preference.
     *
     * @param readPreference the new {@link ReadPreference} for the database
     * @return a new MongoCluster instance with the different readPreference
     * @since CSOT
     */
    MongoCluster withReadPreference(ReadPreference readPreference);

    /**
     * Create a new MongoCluster instance with a different write concern.
     *
     * @param writeConcern the new {@link WriteConcern} for the database
     * @return a new MongoCluster instance with the different writeConcern
     * @since CSOT
     */
    MongoCluster withWriteConcern(WriteConcern writeConcern);

    /**
     * Create a new MongoCluster instance with a different read concern.
     *
     * @param readConcern the new {@link ReadConcern} for the database
     * @return a new MongoCluster instance with the different ReadConcern
     * @since CSOT
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    MongoCluster withReadConcern(ReadConcern readConcern);

    /**
     * Create a new MongoCluster instance with the set time limit for the full execution of an operation.
     *
     * <ul>
     *   <li>{@code 0} means infinite timeout.</li>
     *    <li>{@code > 0} The time limit to use for the full execution of an operation.</li>
     * </ul>
     *
     * @param timeout the timeout, which must be greater than or equal to 0
     * @param timeUnit the time unit
     * @return a new MongoCluster instance with the set time limit for the full execution of an operation.
     * @since CSOT
     * @see #getTimeout
     */
    @Alpha(Alpha.Reason.CLIENT)
    MongoCluster withTimeout(long timeout, TimeUnit timeUnit);

    /**
     * Gets a {@link MongoDatabase} instance for the given database name.
     *
     * @param databaseName the name of the database to retrieve
     * @return a {@code MongoDatabase} representing the specified database
     * @throws IllegalArgumentException if databaseName is invalid
     * @see MongoNamespace#checkDatabaseNameValidity(String)
     */
    MongoDatabase getDatabase(String databaseName);

    /**
     * Creates a client session with default options.
     *
     * <p>Note: A ClientSession instance can not be used concurrently in multiple operations.</p>
     *
     * @return the client session
     * @mongodb.server.release 3.6
     */
    ClientSession startSession();

    /**
     * Creates a client session.
     *
     * <p>Note: A ClientSession instance can not be used concurrently in multiple operations.</p>
     *
     * @param options  the options for the client session
     * @return the client session
     * @mongodb.server.release 3.6
     */
    ClientSession startSession(ClientSessionOptions options);

    /**
     * Get a list of the database names
     *
     * @return an iterable containing all the names of all the databases
     * @mongodb.driver.manual reference/command/listDatabases List Databases
     */
    MongoIterable<String> listDatabaseNames();

    /**
     * Get a list of the database names
     *
     * @param clientSession the client session with which to associate this operation
     * @return an iterable containing all the names of all the databases
     * @mongodb.driver.manual reference/command/listDatabases List Databases
     * @mongodb.server.release 3.6
     */
    MongoIterable<String> listDatabaseNames(ClientSession clientSession);

    /**
     * Gets the list of databases
     *
     * @return the list databases iterable interface
     */
    ListDatabasesIterable<Document> listDatabases();

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list databases iterable interface
     * @mongodb.driver.manual reference/command/listDatabases List Databases
     * @mongodb.server.release 3.6
     */
    ListDatabasesIterable<Document> listDatabases(ClientSession clientSession);

    /**
     * Gets the list of databases
     *
     * @param resultClass the class to cast the database documents to
     * @param <TResult>   the type of the class to use instead of {@code Document}.
     * @return the list databases iterable interface
     */
    <TResult> ListDatabasesIterable<TResult> listDatabases(Class<TResult> resultClass);

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to cast the database documents to
     * @param <TResult>   the type of the class to use instead of {@code Document}.
     * @return the list databases iterable interface
     * @mongodb.driver.manual reference/command/listDatabases List Databases
     * @mongodb.server.release 3.6
     */
    <TResult> ListDatabasesIterable<TResult> listDatabases(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Creates a change stream for this client.
     *
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    ChangeStreamIterable<Document> watch();

    /**
     * Creates a change stream for this client.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    <TResult> ChangeStreamIterable<TResult> watch(Class<TResult> resultClass);

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    ChangeStreamIterable<Document> watch(List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     * @mongodb.server.release 4.0
     */
    <TResult> ChangeStreamIterable<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    ChangeStreamIterable<Document> watch(ClientSession clientSession);

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream.
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    ChangeStreamIterable<Document> watch(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    <TResult> ChangeStreamIterable<TResult> watch(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass);
}
