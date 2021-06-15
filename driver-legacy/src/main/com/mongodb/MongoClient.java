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

package com.mongodb;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.event.ClusterListener;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.SingleServerBinding;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.mongodb.internal.connection.ServerAddressHelper.createServerAddress;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * <p>A MongoDB client with internal connection pooling. For most applications, you should have one MongoClient instance for the entire
 * JVM.
 * <p>The following are equivalent, and all connect to the local database running on the default port:</p>
 * <pre>
 * new MongoClient()
 * new MongoClient("mongodb://localhost")
 * new MongoClient("mongodb://localhost:27017");
 * new MongoClient(MongoClientSettings.builder()
 *   .applyConnectionString("mongodb://localhost")
 *   .build())
 * </pre>
 * <p>You can connect to a <a href="https://docs.mongodb.com/manual/replication/">replica set</a> by passing a
 * list of servers to a MongoClient constructor. For example:</p>
 * <pre>
 * new MongoClient("mongodb://localhost:27017,localhost:27018,localhost:27019")
 * new MongoClient(MongoClientSettings.builder()
 *   .applyConnectionString("mongodb://localhost:27017,localhost:27018,localhost:27019")
 *   .build())
 * </pre>
 * <p>You can connect to a sharded cluster using the same constructor invocations.  MongoClient will auto-detect whether the servers are a
 * list of replica set members or a list of mongos servers.</p>
 *
 * <p>By default, all read and write operations will be made on the primary, but it's possible to read from secondaries by changing the read
 * preference:</p>
 * <pre>
 * new MongoClient("mongodb://localhost:27017,localhost:27018,localhost:27019?readPreference=primary")
 * new MongoClient(MongoClientSettings.builder()
 *   .applyConnectionString("mongodb://localhost:27017,localhost:27018,localhost:27019/?readPreference=primary")
 *   .build())
 * new MongoClient(MongoClientSettings.builder()
 *   .applyConnectionString("mongodb://localhost:27017,localhost:27018,localhost:27019")
 *   .readPreference(ReadPreference.primary())
 *   .build())
 * </pre>
 * <p>By default, all write operations will wait for acknowledgment by the server, as the default write concern is {@code
 * WriteConcern.ACKNOWLEDGED}.  It's possible to change this with a setting:</p>
 * <pre>
 * new MongoClient("mongodb://localhost:27017,localhost:27018,localhost:27019?w=majority")
 * new MongoClient(MongoClientSettings.builder()
 *   .applyConnectionString("mongodb://localhost:27017,localhost:27018,localhost:27019/?w=majority")
 *   .build())
 * new MongoClient(MongoClientSettings.builder()
 *   .applyConnectionString("mongodb://localhost:27017,localhost:27018,localhost:27019")
 *   .writeConcern(WriteConcern.MAJORITY)
 *   .build())
 * </pre>
 * <p>In general, users of this class will pick up all of the default options specified in {@code MongoClientSettings}.
 *
 * @see ConnectionString
 * @see MongoClientSettings
 * @since 2.10.0
 */
public class MongoClient implements Closeable {

    private final ConcurrentMap<String, DB> dbCache = new ConcurrentHashMap<>();

    private final MongoClientOptions options;

    private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();

    private final ConcurrentLinkedQueue<ServerCursorAndNamespace> orphanedCursors = new ConcurrentLinkedQueue<>();
    private final ExecutorService cursorCleaningService;
    private final MongoClientImpl delegate;

    /**
     * Gets the default codec registry.  It includes the following providers:
     *
     * <ul>
     * <li>{@link org.bson.codecs.ValueCodecProvider}</li>
     * <li>{@link org.bson.codecs.BsonValueCodecProvider}</li>
     * <li>{@link com.mongodb.DBRefCodecProvider}</li>
     * <li>{@link com.mongodb.DBObjectCodecProvider}</li>
     * <li>{@link org.bson.codecs.DocumentCodecProvider}</li>
     * <li>{@link org.bson.codecs.IterableCodecProvider}</li>
     * <li>{@link org.bson.codecs.MapCodecProvider}</li>
     * <li>{@link com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider}</li>
     * <li>{@link com.mongodb.client.gridfs.codecs.GridFSFileCodecProvider}</li>
     * <li>{@link org.bson.codecs.jsr310.Jsr310CodecProvider}</li>
     * <li>{@link org.bson.codecs.JsonObjectCodecProvider}</li>
     * <li>{@link org.bson.codecs.BsonCodecProvider}</li>
     * </ul>
     *
     * @return the default codec registry
     * @see MongoClientOptions#getCodecRegistry()
     * @see MongoClientSettings#getDefaultCodecRegistry()
     * @since 3.0
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return com.mongodb.MongoClientSettings.getDefaultCodecRegistry();
    }

    /**
     * Creates an instance based on a (single) MongoDB server ({@code "mongodb://127.0.0.1:27017"}).
     */
    public MongoClient() {
        this(new ConnectionString("mongodb://127.0.0.1"));
    }

    /**
     * Creates a MongoClient instance based on a connection string.
     *
     * @param connectionString server to connect to in connection string format.  For backwards compatibility, the
     * {@code "mongodb://"} prefix can be omitted
     * @see ConnectionString
     */
    public MongoClient(final String connectionString) {
        this(connectionString.contains("://")
                ? new ConnectionString(connectionString) : new ConnectionString("mongodb://" + connectionString));
    }

    /**
     * Create a new client with the given connection string.
     *
     * <p>
     * For each of the settings classed configurable via {@link MongoClientSettings}, the connection string is applied by calling the
     * {@code applyConnectionString} method on an instance of setting's builder class, building the setting, and adding it to an instance of
     * {@link com.mongodb.MongoClientSettings.Builder}.
     * </p>
     *
     * @param connectionString the connection string
     * @see com.mongodb.MongoClientSettings.Builder#applyConnectionString(ConnectionString)
     * @since 4.2
     */
    public MongoClient(final ConnectionString connectionString) {
        this(connectionString, null);
    }

    /**
     * Create a new client with the given connection string.
     *
     * <p>
     * For each of the settings classed configurable via {@link MongoClientSettings}, the connection string is applied by calling the
     * {@code applyConnectionString} method on an instance of setting's builder class, building the setting, and adding it to an instance of
     * {@link com.mongodb.MongoClientSettings.Builder}.
     * </p>
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param connectionString       the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @since 4.2
     */
    public MongoClient(final ConnectionString connectionString,
                       @Nullable final MongoDriverInformation mongoDriverInformation) {
        this(MongoClientSettings.builder().applyConnectionString(connectionString).build(), mongoDriverInformation);
    }

    /**
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @since 4.2
     */
    public MongoClient(final MongoClientSettings settings) {
        this(settings, null);
    }

    /**
     * Creates a new client with the given client settings.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param settings               the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @since 4.2
     */
    public MongoClient(final MongoClientSettings settings, @Nullable final MongoDriverInformation mongoDriverInformation) {
        this(settings, null, mongoDriverInformation);
    }

    private MongoClient(final MongoClientSettings settings,
                       @Nullable final MongoClientOptions options,
                       @Nullable final MongoDriverInformation mongoDriverInformation) {
        delegate = new MongoClientImpl(settings, wrapMongoDriverInformation(mongoDriverInformation));
        this.options = options != null ? options : MongoClientOptions.builder(settings).build();
        cursorCleaningService = this.options.isCursorFinalizerEnabled() ? createCursorCleaningService() : null;
    }

    private static MongoDriverInformation wrapMongoDriverInformation(@Nullable final MongoDriverInformation mongoDriverInformation) {
        return (mongoDriverInformation == null ? MongoDriverInformation.builder() : MongoDriverInformation.builder(mongoDriverInformation))
                .driverName("legacy").build();
    }


    /**
     * Creates an instance based on a (single) mongodb node (default port).
     *
     * @param host    server to connect to in format host[:port]
     * @param options default query options
     */
    public MongoClient(final String host, final MongoClientOptions options) {
        this(createServerAddress(host), options);
    }

    /**
     * Creates an instance based on a (single) mongodb node.
     *
     * @param host the database's host address
     * @param port the port on which the database is running
     */
    public MongoClient(final String host, final int port) {
        this(createServerAddress(host, port));
    }

    /**
     * Creates an instance based on a (single) mongodb node
     *
     * @param addr the database address
     * @see com.mongodb.ServerAddress
     */
    public MongoClient(final ServerAddress addr) {
        this(addr, MongoClientOptions.builder().build());
    }

    /**
     * Creates an instance based on a (single) mongo node using a given ServerAddress and default options.
     *
     * @param addr    the database address
     * @param options default options
     * @see com.mongodb.ServerAddress
     */
    public MongoClient(final ServerAddress addr, final MongoClientOptions options) {
        this(addr, null, options);
    }

    /**
     * Creates an instance based on a (single) mongo node using a given server address, credential, and options
     *
     * @param addr       the database address
     * @param credential the credential used to authenticate all connections
     * @param options    default options
     * @see com.mongodb.ServerAddress
     * @since 3.6
     */
    public MongoClient(final ServerAddress addr, @Nullable final MongoCredential credential, final MongoClientOptions options) {
        this(addr, credential, options, null);
    }

    /**
     * <p>Creates an instance based on a list of replica set members or mongos servers. For a replica set it will discover all members.
     * For a list with a single seed, the driver will still discover all members of the replica set.  For a direct
     * connection to a replica set member, with no discovery, use the {@link #MongoClient(ServerAddress)} constructor instead.</p>
     *
     * <p>When there is more than one server to choose from based on the type of request (read or write) and the read preference (if it's a
     * read request), the driver will randomly select a server to send a request. This applies to both replica sets and sharded clusters.
     * The servers to randomly select from are further limited by the local threshold.  See
     * {@link MongoClientOptions#getLocalThreshold()}</p>
     *
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @see MongoClientOptions#getLocalThreshold()
     */
    public MongoClient(final List<ServerAddress> seeds) {
        this(seeds, MongoClientOptions.builder().build());
    }

    /**
     * <p>Construct an instance based on a list of replica set members or mongos servers. For a replica set it will discover all members.
     * For a list with a single seed, the driver will still discover all members of the replica set.  For a direct
     * connection to a replica set member, with no discovery, use the {@link #MongoClient(ServerAddress, MongoClientOptions)} constructor
     * instead.</p>
     *
     * <p>When there is more than one server to choose from based on the type of request (read or write) and the read preference (if it's a
     * read request), the driver will randomly select a server to send a request. This applies to both replica sets and sharded clusters.
     * The servers to randomly select from are further limited by the local threshold.  See
     * {@link MongoClientOptions#getLocalThreshold()}</p>
     *
     * @param seeds   Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of
     *                mongod servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param options the options
     * @see MongoClientOptions#getLocalThreshold()
     */
    public MongoClient(final List<ServerAddress> seeds, final MongoClientOptions options) {
        this(seeds, null, options);
    }

    /**
     * <p>Creates an instance based on a list of replica set members or mongos servers. For a replica set it will discover all members.
     * For a list with a single seed, the driver will still discover all members of the replica set.  For a direct
     * connection to a replica set member, with no discovery, use the
     * {@link #MongoClient(ServerAddress, MongoCredential, MongoClientOptions)} constructor instead.</p>
     *
     * <p>When there is more than one server to choose from based on the type of request (read or write) and the read preference (if it's a
     * read request), the driver will randomly select a server to send a request. This applies to both replica sets and sharded clusters.
     * The servers to randomly select from are further limited by the local threshold.  See
     * {@link MongoClientOptions#getLocalThreshold()}</p>
     *
     * @param seeds      Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of
     *                   mongod servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param credential the credential used to authenticate all connections
     * @param options    the options
     * @see MongoClientOptions#getLocalThreshold()
     * @since 3.6
     */
    public MongoClient(final List<ServerAddress> seeds, @Nullable final MongoCredential credential, final MongoClientOptions options) {
        this(seeds, credential, options, null);
    }

    /**
     * Creates an instance described by a URI. If only one address is used it will only connect to that node, otherwise it will discover all
     * nodes.
     *
     * @param uri the URI
     * @throws MongoException if theres a failure
     */
    public MongoClient(final MongoClientURI uri) {
        this(uri, null);
    }

    /**
     * Creates an instance described by a URI.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param uri                    the URI
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @throws MongoException if theres a failure
     * @since 3.4
     */
    public MongoClient(final MongoClientURI uri, @Nullable final MongoDriverInformation mongoDriverInformation) {
        this(uri.getOptions().asMongoClientSettings(
                uri.getProxied().isSrvProtocol()
                        ? null : uri.getProxied().getHosts().stream().map(ServerAddress::new).collect(Collectors.toList()),
                uri.getProxied().isSrvProtocol()
                        ? uri.getProxied().getHosts().get(0) : null,
                getClusterConnectionMode(uri.getProxied()),
                uri.getCredentials()),
                uri.getOptions(),
                mongoDriverInformation);
    }

    private static ClusterConnectionMode getClusterConnectionMode(final ConnectionString connectionString) {
        return ClusterSettings.builder().applyConnectionString(connectionString).build().getMode();
    }

    /**
     * Creates a MongoClient to a single node using a given ServerAddress.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param addr                   the database address
     * @param credential             the credential used to authenticate all connections
     * @param options                default options
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @see com.mongodb.ServerAddress
     * @since 3.6
     */
    public MongoClient(final ServerAddress addr, @Nullable final MongoCredential credential, final MongoClientOptions options,
                       @Nullable final MongoDriverInformation mongoDriverInformation) {
        this(options.asMongoClientSettings(singletonList(addr), null, ClusterConnectionMode.SINGLE, credential), options,
                mongoDriverInformation);
    }

    /**
     * Creates a MongoClient
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param seeds                  Put as many servers as you can in the list and the system will figure out the rest.  This can either
     *                               be a list of mongod servers in the same replica set or a list of mongos servers in the same sharded
     *                               cluster.
     * @param credential             the credential used to authenticate all connections
     * @param options                the options
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @since 3.6
     */
    public MongoClient(final List<ServerAddress> seeds, @Nullable final MongoCredential credential, final MongoClientOptions options,
                       @Nullable final MongoDriverInformation mongoDriverInformation) {
        this(options.asMongoClientSettings(seeds, null, ClusterConnectionMode.MULTIPLE, credential), options, mongoDriverInformation);
    }

    /**
     * Gets the options that this client uses to connect to server.
     *
     * <p>Note: {@link MongoClientOptions} is immutable.</p>
     *
     * @return the options
     */
    public MongoClientOptions getMongoClientOptions() {
        return options;
    }

    /**
     * Gets the credential that this client authenticates all connections with
     *
     * @return the credential, which may be null in unsecured deployments
     * @since 3.9
     */
    @Nullable
    public MongoCredential getCredential() {
        return delegate.getSettings().getCredential();
    }

    /**
     * Get a list of the database names
     *
     * @return an iterable containing all the names of all the databases
     * @mongodb.driver.manual reference/command/listDatabases List Databases
     * @since 3.0
     */
    public MongoIterable<String> listDatabaseNames() {
        return delegate.listDatabaseNames();
    }

    /**
     * Get a list of the database names
     *
     * @param clientSession the client session with which to associate this operation
     * @return an iterable containing all the names of all the databases
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/listDatabases List Databases
     * @since 3.6
     */
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        return delegate.listDatabaseNames(clientSession);
    }

    /**
     * Gets the list of databases
     *
     * @return the list of databases
     * @since 3.0
     */
    public ListDatabasesIterable<Document> listDatabases() {
        return delegate.listDatabases();
    }

    /**
     * Gets the list of databases
     *
     * @param clazz the class to cast the database documents to
     * @param <T>   the type of the class to use instead of {@code Document}.
     * @return the list of databases
     * @since 3.0
     */
    public <T> ListDatabasesIterable<T> listDatabases(final Class<T> clazz) {
        return delegate.listDatabases(clazz);
    }

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list of databases
     * @mongodb.server.release 3.6
     * @since 3.6
     */
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return delegate.listDatabases(clientSession);
    }

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @param clazz         the class to cast the database documents to
     * @param <T>           the type of the class to use instead of {@code Document}.
     * @return the list of databases
     * @mongodb.server.release 3.6
     * @since 3.6
     */
    public <T> ListDatabasesIterable<T> listDatabases(final ClientSession clientSession, final Class<T> clazz) {
        return delegate.listDatabases(clientSession, clazz);
    }


    /**
     * @param databaseName the name of the database to retrieve
     * @return a {@code MongoDatabase} representing the specified database
     * @throws IllegalArgumentException if databaseName is invalid
     * @see MongoNamespace#checkDatabaseNameValidity(String)
     */
    public MongoDatabase getDatabase(final String databaseName) {
        return delegate.getDatabase(databaseName);
    }

    /**
     * Creates a client session with default session options.
     *
     * @return the client session
     * @throws MongoClientException if the MongoDB cluster to which this client is connected does not support sessions
     * @mongodb.server.release 3.6
     * @since 3.8
     */
    public ClientSession startSession() {
        return delegate.startSession();
    }

    /**
     * Creates a client session.
     *
     * @param options the options for the client session
     * @return the client session
     * @throws MongoClientException if the MongoDB cluster to which this client is connected does not support sessions
     * @mongodb.server.release 3.6
     * @since 3.6
     */
    public ClientSession startSession(final ClientSessionOptions options) {
        return delegate.startSession(options);
    }

    /**
     * Creates a change stream for this client.
     *
     * @return the change stream iterable
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     */
    public ChangeStreamIterable<Document> watch() {
        return delegate.watch();
    }

    /**
     * Creates a change stream for this client.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     */
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return delegate.watch(resultClass);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline the aggregation pipeline to apply to the change stream
     * @return the change stream iterable
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     */
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return delegate.watch(pipeline);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     */
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return delegate.watch(pipeline, resultClass);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the change stream iterable
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     */
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return delegate.watch(clientSession);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param resultClass   the class to decode each document into
     * @param <TResult>     the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     */
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return delegate.watch(clientSession, resultClass);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline      the aggregation pipeline to apply to the change stream
     * @return the change stream iterable
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     */
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return delegate.watch(clientSession, pipeline);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline      the aggregation pipeline to apply to the change stream
     * @param resultClass   the class to decode each document into
     * @param <TResult>     the target document type of the iterable.
     * @return the change stream iterable
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     * @since 3.8
     */
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        return delegate.watch(clientSession, pipeline, resultClass);
    }

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
     * @since 4.2
     */
    public ClusterDescription getClusterDescription() {
        return delegate.getClusterDescription();
    }

    /**
     * Gets the write concern
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return options.getWriteConcern();
    }

    /**
     * Gets the read concern
     *
     * @return the read concern
     */
    public ReadConcern getReadConcern() {
        return options.getReadConcern();
    }

    /**
     * Gets the default read preference
     *
     * @return the default read preference
     */
    public ReadPreference getReadPreference() {
        return options.getReadPreference();
    }

    /**
     * Gets a database object. Users should use {@link com.mongodb.MongoClient#getDatabase(String)} instead.
     *
     * <p>
     * The {@link DB} class has been superseded by {@link com.mongodb.client.MongoDatabase}.  The deprecation of this method effectively
     * deprecates the {@link DB}, {@link DBCollection}, and {@link DBCursor} classes, among others; but in order to give users time to
     * migrate to the new API without experiencing a huge number of compiler warnings, those classes have not yet been formally
     * deprecated.
     * </p>
     *
     * @param dbName the name of the database to retrieve
     * @return a DB representing the specified database
     * @throws IllegalArgumentException if the name is invalid
     * @see MongoNamespace#checkDatabaseNameValidity(String)
     * @deprecated This method is not currently scheduled for removal, but prefer {@link com.mongodb.MongoClient#getDatabase(String)} for
     * new code.  Note that {@link DB} and {@link com.mongodb.client.MongoDatabase} can be used together in the same application, with the
     * same instance.
     */
    @Deprecated // NOT CURRENTLY INTENDED FOR REMOVAL
    public DB getDB(final String dbName) {
        DB db = dbCache.get(dbName);
        if (db != null) {
            return db;
        }

        db = new DB(this, dbName, getOperationExecutor());
        DB temp = dbCache.putIfAbsent(dbName, db);
        if (temp != null) {
            return temp;
        }
        return db;
    }

    /**
     * Drops the database if it exists.
     *
     * @param dbName name of database to drop
     * @throws MongoException if the operation fails
     */
    public void dropDatabase(final String dbName) {
        getDB(dbName).dropDatabase();
    }

    /**
     * Closes all resources associated with this instance, in particular any open network connections. Once called, this instance and any
     * databases obtained from it can no longer be used.
     */
    public void close() {
        delegate.close();
        if (cursorCleaningService != null) {
            cursorCleaningService.shutdownNow();
        }
    }

    @Override
    public String toString() {
        return "MongoClient{"
                + "options=" + options
                + '}';
    }

    Cluster getCluster() {
        return delegate.getCluster();
    }

    CodecRegistry getCodecRegistry() {
        return delegate.getCodecRegistry();
    }

    ServerSessionPool getServerSessionPool() {
        return delegate.getServerSessionPool();
    }

    BufferProvider getBufferProvider() {
        return bufferProvider;
    }

    @Nullable
    ExecutorService getCursorCleaningService() {
        return cursorCleaningService;
    }

    void addOrphanedCursor(final ServerCursor serverCursor, final MongoNamespace namespace) {
        orphanedCursors.add(new ServerCursorAndNamespace(serverCursor, namespace));
    }

    // Leave as package-protected so that unit tests can spy on it.
    OperationExecutor getOperationExecutor() {
        return delegate.getOperationExecutor();
    }

    MongoClientImpl getDelegate() {
        return delegate;
    }

    private ExecutorService createCursorCleaningService() {
        ScheduledExecutorService newTimer = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("CleanCursors"));
        newTimer.scheduleAtFixedRate(this::cleanCursors, 1, 1, SECONDS);
        return newTimer;
    }

    private void cleanCursors() {
        ServerCursorAndNamespace cur;
        while ((cur = orphanedCursors.poll()) != null) {
            ReadWriteBinding binding = new SingleServerBinding(delegate.getCluster(), cur.serverCursor.getAddress(),
                    options.getServerApi());
            try {
                ConnectionSource source = binding.getReadConnectionSource();
                try {
                    Connection connection = source.getConnection();
                    try {
                        connection.killCursor(cur.namespace, singletonList(cur.serverCursor.getId()));
                    } finally {
                        connection.release();
                    }
                } finally {
                    source.release();
                }
            } finally {
                binding.release();
            }
        }
    }

    private static class ServerCursorAndNamespace {
        private final ServerCursor serverCursor;
        private final MongoNamespace namespace;

        ServerCursorAndNamespace(final ServerCursor serverCursor, final MongoNamespace namespace) {
            this.serverCursor = serverCursor;
            this.namespace = namespace;
        }
    }
}
