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

package com.mongodb.async.client;

import com.mongodb.ClientSessionOptions;
import com.mongodb.annotations.Immutable;
import com.mongodb.async.SingleResultCallback;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.Closeable;
import java.util.List;

/**
 * A client-side representation of a MongoDB cluster.  Instances can represent either a standalone MongoDB instance, a replica set,
 * or a sharded cluster.  Instance of this class are responsible for maintaining an up-to-date state of the cluster,
 * and possibly cache resources related to this, including background threads for monitoring, and connection pools.
 * <p>
 * Instance of this class serve as factories for {@code MongoDatabase} instances.
 * </p>
 * @since 3.0
 * @deprecated Prefer the Reactive Streams-based asynchronous driver (mongodb-driver-reactivestreams artifactId)
 */
@Immutable
@Deprecated
public interface MongoClient extends Closeable {

    /**
     * Creates a client session with default options.
     *
     * <p>Note: A ClientSession instance can not be used concurrently in multiple asynchronous operations.</p>
     *
     * @param callback the callback that is passed the clientSession or a {@code MongoClientException} if the MongoDB cluster to which
     *                 this client is connected does not support sessions
     * @mongodb.server.release 3.6
     * @since 3.8
     */
    void startSession(SingleResultCallback<ClientSession> callback);

    /**
     * Creates a client session.
     *
     * <p>Note: A ClientSession instance can not be used concurrently in multiple asynchronous operations.</p>
     *
     * @param options  the options for the client session
     * @param callback the callback that is passed the clientSession or a {@code MongoClientException} if the MongoDB cluster to which
     *                 this client is connected does not support sessions
     * @mongodb.server.release 3.6
     * @since 3.6
     */
    void startSession(ClientSessionOptions options, SingleResultCallback<ClientSession> callback);

    /**
     * Gets the database with the given name.
     *
     * @param name the name of the database
     * @return the database
     * @throws IllegalArgumentException if databaseName is invalid
     * @see com.mongodb.MongoNamespace#checkDatabaseNameValidity(String)
     */
    MongoDatabase getDatabase(String name);

    /**
     * Close the client, which will close all underlying cached resources, including, for example,
     * sockets and background monitoring threads.
     */
    void close();

    /**
     * Gets the settings that this client uses to connect to server.
     *
     * <p>Note: {@link MongoClientSettings} is immutable.</p>
     *
     * @return the settings
     * @deprecated this method is being removed
     */
    @Deprecated
    MongoClientSettings getSettings();

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
     * @since 3.6
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
     * @since 3.6
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
     * @since 3.6
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
