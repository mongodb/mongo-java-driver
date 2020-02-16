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

import com.mongodb.ClientSessionOptions;
import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.event.ClusterListener;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;

import java.io.Closeable;
import java.util.List;

/**
 * A client-side representation of a MongoDB cluster.  Instances can represent either a standalone MongoDB instance, a replica set,
 * or a sharded cluster.  Instance of this class are responsible for maintaining an up-to-date state of the cluster,
 * and possibly cache resources related to this, including background threads for monitoring, and connection pools.
 * <p>
 * Instance of this class server as factories for {@code MongoDatabase} instances.
 * </p>
 *
 * @since 1.0
 */
@Immutable
public interface MongoClient extends Closeable {
    /**
     * Gets the database with the given name.
     *
     * @param name the name of the database
     * @return the database
     */
    MongoDatabase getDatabase(String name);

    /**
     * Close the client, which will close all underlying cached resources, including, for example,
     * sockets and background monitoring threads.
     */
    void close();

    /**
     * Get a list of the database names
     *
     * @mongodb.driver.manual reference/commands/listDatabases List Databases
     * @return an iterable containing all the names of all the databases
     */
    Publisher<String> listDatabaseNames();

    /**
     * Get a list of the database names
     *
     * @param clientSession the client session with which to associate this operation
     * @mongodb.driver.manual reference/commands/listDatabases List Databases
     * @return an iterable containing all the names of all the databases
     *
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<String> listDatabaseNames(ClientSession clientSession);

    /**
     * Gets the list of databases
     *
     * @return the fluent list databases interface
     */
    ListDatabasesPublisher<Document> listDatabases();

    /**
     * Gets the list of databases
     *
     * @param clazz the class to cast the database documents to
     * @param <TResult>   the type of the class to use instead of {@code Document}.
     * @return the fluent list databases interface
     */
    <TResult> ListDatabasesPublisher<TResult> listDatabases(Class<TResult> clazz);

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @return the fluent list databases interface
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    ListDatabasesPublisher<Document> listDatabases(ClientSession clientSession);

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @param clazz the class to cast the database documents to
     * @param <TResult>   the type of the class to use instead of {@code Document}.
     * @return the fluent list databases interface
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    <TResult> ListDatabasesPublisher<TResult> listDatabases(ClientSession clientSession, Class<TResult> clazz);

    /**
     * Creates a change stream for this client.
     *
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 1.9
     * @mongodb.server.release 4.0
     */
    ChangeStreamPublisher<Document> watch();

    /**
     * Creates a change stream for this client.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 1.9
     * @mongodb.server.release 4.0
     */
    <TResult> ChangeStreamPublisher<TResult> watch(Class<TResult> resultClass);

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 1.9
     * @mongodb.server.release 4.0
     */
    ChangeStreamPublisher<Document> watch(List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 1.9
     * @mongodb.server.release 4.0
     */
    <TResult> ChangeStreamPublisher<TResult> watch(List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the change stream iterable
     * @since 1.9
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    ChangeStreamPublisher<Document> watch(ClientSession clientSession);

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 1.9
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    <TResult> ChangeStreamPublisher<TResult> watch(ClientSession clientSession, Class<TResult> resultClass);

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline the aggregation pipeline to apply to the change stream.
     * @return the change stream iterable
     * @since 1.9
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    ChangeStreamPublisher<Document> watch(ClientSession clientSession, List<? extends Bson> pipeline);

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 1.9
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    <TResult> ChangeStreamPublisher<TResult> watch(ClientSession clientSession, List<? extends Bson> pipeline, Class<TResult> resultClass);

    /**
     * Creates a client session.
     *
     * @return a publisher for the client session.
     * @mongodb.server.release 3.6
     * @since 1.9
     */
    Publisher<ClientSession> startSession();

    /**
     * Creates a client session.
     *
     * @param options the options for the client session
     * @return a publisher for the client session.
     * @mongodb.server.release 3.6
     * @since 1.7
     */
    Publisher<ClientSession> startSession(ClientSessionOptions options);

    /**
     * Gets the current cluster description.
     *
     * <p>
     * This method will not block, meaning that it may return a {@link ClusterDescription} whose {@code clusterType} is unknown
     * and whose {@link com.mongodb.connection.ServerDescription}s are all in the connecting state.  If the application requires
     * notifications after the driver has connected to a member of the cluster, it should register a {@link ClusterListener} via
     * the {@link ClusterSettings} in {@link com.mongodb.MongoClientSettings}.
     * </p>
     *
     * @return the current cluster description
     * @see ClusterSettings.Builder#addClusterListener(ClusterListener)
     * @see com.mongodb.MongoClientSettings.Builder#applyToClusterSettings(com.mongodb.Block)
     * @since 4.1
     */
    ClusterDescription getClusterDescription();
}
