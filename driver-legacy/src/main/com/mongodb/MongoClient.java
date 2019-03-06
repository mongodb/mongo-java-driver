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

import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.binding.SingleServerBinding;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.ClientSession;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.internal.MongoClientDelegate;
import com.mongodb.client.internal.MongoDatabaseImpl;
import com.mongodb.client.internal.MongoIterables;
import com.mongodb.client.internal.SimpleMongoClient;
import com.mongodb.client.internal.OperationExecutor;
import com.mongodb.client.model.changestream.ChangeStreamLevel;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.Connection;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.event.ClusterListener;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.client.internal.Crypts.createCrypt;
import static com.mongodb.internal.connection.ServerAddressHelper.createServerAddress;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.bson.internal.CodecRegistryHelper.createRegistry;

/**
 * <p>A MongoDB client with internal connection pooling. For most applications, you should have one MongoClient instance for the entire
 * JVM.
 * <p>The following are equivalent, and all connect to the local database running on the default port:</p>
 * <pre>
 * MongoClient mongoClient1 = new MongoClient();
 * MongoClient mongoClient1 = new MongoClient("localhost");
 * MongoClient mongoClient2 = new MongoClient("localhost", 27017);
 * MongoClient mongoClient4 = new MongoClient(new ServerAddress("localhost"));
 * MongoClient mongoClient5 = new MongoClient(new ServerAddress("localhost"), MongoClientOptions.builder().build());
 * </pre>
 * <p>You can connect to a <a href="http://www.mongodb.org/display/DOCS/Replica+Sets">replica set</a> using the Java driver by passing a
 * ServerAddress list to the MongoClient constructor. For example:</p>
 * <pre>
 * MongoClient mongoClient = new MongoClient(Arrays.asList(
 *   new ServerAddress("localhost", 27017),
 *   new ServerAddress("localhost", 27018),
 *   new ServerAddress("localhost", 27019)));
 * </pre>
 * <p>You can connect to a sharded cluster using the same constructor.  MongoClient will auto-detect whether the servers are a list of
 * replica set members or a list of mongos servers.</p>
 *
 * <p>By default, all read and write operations will be made on the primary, but it's possible to read from secondaries by changing the read
 * preference:</p>
 * <pre>
 * mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
 * </pre>
 * <p>By default, all write operations will wait for acknowledgment by the server, as the default write concern is {@code
 * WriteConcern.ACKNOWLEDGED}.</p>
 *
 * <p>Note: This class supersedes the {@code Mongo} class.  While it extends {@code Mongo}, it differs from it in that the default write
 * concern is to wait for acknowledgment from the server of all write operations.  In addition, its constructors accept instances of {@code
 * MongoClientOptions} and {@code MongoClientURI}, which both also set the same default write concern.</p>
 *
 * <p>In general, users of this class will pick up all of the default options specified in {@code MongoClientOptions}.
 *
 * @see ReadPreference#primary()
 * @see com.mongodb.WriteConcern#ACKNOWLEDGED
 * @see MongoClientOptions
 * @see MongoClientURI
 * @since 2.10.0
 */
@SuppressWarnings("deprecation")
public class MongoClient implements Closeable {

    private final ConcurrentMap<String, DB> dbCache = new ConcurrentHashMap<>();

    private final MongoClientOptions options;
    private final MongoCredential credential;

    private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();

    private final ConcurrentLinkedQueue<ServerCursorAndNamespace> orphanedCursors = new ConcurrentLinkedQueue<>();
    private final ExecutorService cursorCleaningService;
    private final MongoClientDelegate delegate;

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
     * <li>{@link org.bson.codecs.BsonCodecProvider}</li>
     * </ul>
     *
     * @return the default codec registry
     * @see MongoClientOptions#getCodecRegistry()
     * @since 3.0
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return com.mongodb.MongoClientSettings.getDefaultCodecRegistry();
    }

    /**
     * Creates an instance based on a (single) mongodb node (localhost, default port).
     */
    public MongoClient() {
        this(new ServerAddress());
    }

    /**
     * Creates a MongoClient instance based on a (single) mongodb node.
     *
     * @param host server to connect to in format host[:port]
     */
    public MongoClient(final String host) {
        this(createServerAddress(host));
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
        this(createCluster(addr, null, options, null), options, null);
    }

    /**
     * Creates an instance based on a (single) mongo node using a given server address, credential, and options
     *
     * @param addr            the database address
     * @param credential      the credential used to authenticate all connections
     * @param options         default options
     * @see com.mongodb.ServerAddress
     * @since 3.6.0
     */
    public MongoClient(final ServerAddress addr, final MongoCredential credential, final MongoClientOptions options) {
        this(createCluster(addr, credential, options, null), options, credential);
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
        this(seeds, new MongoClientOptions.Builder().build());
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
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param options the options
     * @see MongoClientOptions#getLocalThreshold()
     */
    public MongoClient(final List<ServerAddress> seeds, final MongoClientOptions options) {
        this(createCluster(seeds, null, options, null), options, null);
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
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param credential the credential used to authenticate all connections
     * @param options         the options
     * @see MongoClientOptions#getLocalThreshold()
     * @since 3.6.0
     */
    public MongoClient(final List<ServerAddress> seeds, final MongoCredential credential, final MongoClientOptions options) {
        this(createCluster(seeds, credential, options, null), options, credential);
    }

    /**
     * Creates an instance described by a URI. If only one address is used it will only connect to that node, otherwise it will discover all
     * nodes.
     *
     * @param uri the URI
     * @throws MongoException if theres a failure
     */
    public MongoClient(final MongoClientURI uri) {
        this(createCluster(uri, null), uri.getOptions(), uri.getCredentials());
    }

    /**
     * Creates an instance described by a URI.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param uri the URI
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @throws MongoException if theres a failure
     * @since 3.4
     */
    public MongoClient(final MongoClientURI uri, final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(uri, mongoDriverInformation), uri.getOptions(), uri.getCredentials());
    }

    /**
     * Creates a MongoClient to a single node using a given ServerAddress.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param addr            the database address
     * @param credential      the credential used to authenticate all connections
     * @param options         default options
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @see com.mongodb.ServerAddress
     * @since 3.6
     */
    public MongoClient(final ServerAddress addr, final MongoCredential credential, final MongoClientOptions options,
                       final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(addr, credential, options, mongoDriverInformation), options, credential);
    }

    /**
     * Creates a MongoClient
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param credential      the credential used to authenticate all connections
     * @param options         the options
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @since 3.6
     */
    public MongoClient(final List<ServerAddress> seeds, final MongoCredential credential, final MongoClientOptions options,
                       final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(seeds, credential, options, mongoDriverInformation), options, credential);
    }

    MongoClient(final Cluster cluster, final MongoClientOptions options, @Nullable final MongoCredential credential) {
        this.options = options;
        this.credential = credential;

        AutoEncryptionSettings autoEncryptionSettings = options.getAutoEncryptionSettings();
        this.delegate = new MongoClientDelegate(cluster, createRegistry(options.getCodecRegistry(), options.getUuidRepresentation()), this,
                autoEncryptionSettings == null ? null : createCrypt(asSimpleMongoClient(), autoEncryptionSettings));

        cursorCleaningService = options.isCursorFinalizerEnabled() ? createCursorCleaningService() : null;
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
        return credential;
    }

    /**
     * Get a list of the database names
     *
     * @mongodb.driver.manual reference/command/listDatabases List Databases
     * @return an iterable containing all the names of all the databases
     * @since 3.0
     */
    public MongoIterable<String> listDatabaseNames() {
        return createListDatabaseNamesIterable(null);
    }

    /**
     * Get a list of the database names
     *
     * @param clientSession the client session with which to associate this operation
     * @return an iterable containing all the names of all the databases
     * @since 3.6
     * @mongodb.server.release 3.6
     * @mongodb.driver.manual reference/command/listDatabases List Databases
     */
    public MongoIterable<String> listDatabaseNames(final ClientSession clientSession) {
        notNull("clientSession", clientSession);
        return createListDatabaseNamesIterable(clientSession);
    }

    private MongoIterable<String> createListDatabaseNamesIterable(@Nullable final ClientSession clientSession) {
        return createListDatabasesIterable(clientSession, BsonDocument.class).nameOnly(true).map(new Function<BsonDocument, String>() {
            @Override
            public String apply(final BsonDocument result) {
                return result.getString("name").getValue();
            }
        });
    }

    /**
     * Gets the list of databases
     *
     * @return the list of databases
     * @since 3.0
     */
    public ListDatabasesIterable<Document> listDatabases() {
        return listDatabases(Document.class);
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
        return createListDatabasesIterable(null, clazz);
    }

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @return the list of databases
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public ListDatabasesIterable<Document> listDatabases(final ClientSession clientSession) {
        return listDatabases(clientSession, Document.class);
    }

    /**
     * Gets the list of databases
     *
     * @param clientSession the client session with which to associate this operation
     * @param clazz the class to cast the database documents to
     * @param <T>   the type of the class to use instead of {@code Document}.
     * @return the list of databases
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public <T> ListDatabasesIterable<T> listDatabases(final ClientSession clientSession, final Class<T> clazz) {
        notNull("clientSession", clientSession);
        return createListDatabasesIterable(clientSession, clazz);
    }

    private <T> ListDatabasesIterable<T> createListDatabasesIterable(@Nullable final ClientSession clientSession, final Class<T> clazz) {
        return MongoIterables.listDatabasesOf(clientSession, clazz, delegate.getCodecRegistry(),
                ReadPreference.primary(), createOperationExecutor(), getMongoClientOptions().getRetryReads());
    }

    /**
     * @param databaseName the name of the database to retrieve
     * @return a {@code MongoDatabase} representing the specified database
     * @throws IllegalArgumentException if databaseName is invalid
     * @see MongoNamespace#checkDatabaseNameValidity(String)
     */
    public MongoDatabase getDatabase(final String databaseName) {
        MongoClientOptions clientOptions = getMongoClientOptions();
        return new MongoDatabaseImpl(databaseName, delegate.getCodecRegistry(), clientOptions.getReadPreference(),
                clientOptions.getWriteConcern(), clientOptions.getRetryWrites(), clientOptions.getRetryReads(),
                clientOptions.getReadConcern(),
                clientOptions.getUuidRepresentation(),
                createOperationExecutor());
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
        return startSession(ClientSessionOptions.builder().build());
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
        ClientSession clientSession = createClientSession(notNull("options", options));
        if (clientSession == null) {
            throw new MongoClientException("Sessions are not supported by the MongoDB cluster to which this client is connected");
        }
        return clientSession;
    }

    /**
     * Creates a change stream for this client.
     *
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    public ChangeStreamIterable<Document> watch() {
        return watch(Collections.<Bson>emptyList());
    }

    /**
     * Creates a change stream for this client.
     *
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    public <TResult> ChangeStreamIterable<TResult> watch(final Class<TResult> resultClass) {
        return watch(Collections.<Bson>emptyList(), resultClass);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    public ChangeStreamIterable<Document> watch(final List<? extends Bson> pipeline) {
        return watch(pipeline, Document.class);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @param resultClass the class to decode each document into
     * @param <TResult>   the target document type of the iterable.
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    public <TResult> ChangeStreamIterable<TResult> watch(final List<? extends Bson> pipeline, final Class<TResult> resultClass) {
        return createChangeStreamIterable(null, pipeline, resultClass);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession) {
        return watch(clientSession, Collections.<Bson>emptyList(), Document.class);
    }

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
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final Class<TResult> resultClass) {
        return watch(clientSession, Collections.<Bson>emptyList(), resultClass);
    }

    /**
     * Creates a change stream for this client.
     *
     * @param clientSession the client session with which to associate this operation
     * @param pipeline    the aggregation pipeline to apply to the change stream
     * @return the change stream iterable
     * @since 3.8
     * @mongodb.server.release 4.0
     * @mongodb.driver.dochub core/changestreams Change Streams
     */
    public ChangeStreamIterable<Document> watch(final ClientSession clientSession, final List<? extends Bson> pipeline) {
        return watch(clientSession, pipeline, Document.class);
    }

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
    public <TResult> ChangeStreamIterable<TResult> watch(final ClientSession clientSession, final List<? extends Bson> pipeline,
                                                         final Class<TResult> resultClass) {
        notNull("clientSession", clientSession);
        return createChangeStreamIterable(clientSession, pipeline, resultClass);
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

        db = new DB(this, dbName, createOperationExecutor());
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

    private static Cluster createCluster(final MongoClientURI mongoURI, @Nullable final MongoDriverInformation mongoDriverInformation) {
        return createCluster(
                getClusterSettings(ClusterSettings.builder().applyConnectionString(mongoURI.getProxied()), mongoURI.getOptions()),
                mongoURI.getCredentials(), mongoURI.getOptions(), mongoDriverInformation);
    }

    private static Cluster createCluster(final List<ServerAddress> seedList,
                                         @Nullable final MongoCredential credential, final MongoClientOptions options,
                                         @Nullable final MongoDriverInformation mongoDriverInformation) {
        return createCluster(getClusterSettings(seedList, options, ClusterConnectionMode.MULTIPLE), credential, options,
                mongoDriverInformation);
    }

    private static Cluster createCluster(final ServerAddress serverAddress, @Nullable final MongoCredential credential,
                                         final MongoClientOptions options, @Nullable final MongoDriverInformation mongoDriverInformation) {
        return createCluster(getClusterSettings(singletonList(serverAddress), options, getSingleServerClusterMode(options)),
                credential, options, mongoDriverInformation);
    }

    private static Cluster createCluster(final ClusterSettings clusterSettings, @Nullable final MongoCredential credential,
                                         final MongoClientOptions options, @Nullable final MongoDriverInformation mongoDriverInformation) {
        return new DefaultClusterFactory().createCluster(clusterSettings,
                options.getServerSettings(),
                options.getConnectionPoolSettings(),
                new SocketStreamFactory(options.getSocketSettings(),
                        options.getSslSettings()),
                new SocketStreamFactory(options.getHeartbeatSocketSettings(),
                        options.getSslSettings()),
                credential == null ? Collections.emptyList() : Collections.singletonList(credential),
                getCommandListener(options.getCommandListeners()),
                options.getApplicationName(),
                mongoDriverInformation,
                options.getCompressorList());
    }

    private static ClusterSettings getClusterSettings(final ClusterSettings.Builder builder, final MongoClientOptions options) {
        builder.requiredReplicaSetName(options.getRequiredReplicaSetName())
                .serverSelectionTimeout(options.getServerSelectionTimeout(), MILLISECONDS)
                .localThreshold(options.getLocalThreshold(), MILLISECONDS)
                .serverSelector(options.getServerSelector())
                .maxWaitQueueSize(options.getConnectionPoolSettings().getMaxWaitQueueSize());
        for (ClusterListener clusterListener: options.getClusterListeners()) {
            builder.addClusterListener(clusterListener);
        }
        return builder.build();
    }

    private static ClusterSettings getClusterSettings(final List<ServerAddress> seedList, final MongoClientOptions options,
                                                      final ClusterConnectionMode clusterConnectionMode) {
        return getClusterSettings(ClusterSettings.builder()
                .hosts(new ArrayList<ServerAddress>(seedList))
                .mode(clusterConnectionMode), options);
    }

    MongoClientDelegate getDelegate() {
        return delegate;
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

    void addOrphanedCursor(final ServerCursor serverCursor, final MongoNamespace namespace) {
        orphanedCursors.add(new ServerCursorAndNamespace(serverCursor, namespace));
    }

    // Leave as package-protected so that unit tests can spy on it.
    OperationExecutor createOperationExecutor() {
        return delegate.getOperationExecutor();
    }

    @Nullable
    private ClientSession createClientSession(final ClientSessionOptions clientSessionOptions) {
        return delegate.createClientSession(clientSessionOptions, options.getReadConcern(), options.getWriteConcern(),
                options.getReadPreference());
    }

    private ExecutorService createCursorCleaningService() {
        ScheduledExecutorService newTimer = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("CleanCursors"));
        newTimer.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                cleanCursors();
            }
        }, 1, 1, SECONDS);
        return newTimer;
    }

    private void cleanCursors() {
        ServerCursorAndNamespace cur;
        while ((cur = orphanedCursors.poll()) != null) {
            ReadWriteBinding binding = new SingleServerBinding(delegate.getCluster(), cur.serverCursor.getAddress());
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

    private static ClusterConnectionMode getSingleServerClusterMode(final MongoClientOptions options) {
        if (options.getRequiredReplicaSetName() == null) {
            return ClusterConnectionMode.SINGLE;
        } else {
            return ClusterConnectionMode.MULTIPLE;
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

    private SimpleMongoClient asSimpleMongoClient() {
        return new SimpleMongoClient() {
            @Override
            public MongoDatabase getDatabase(final String databaseName) {
                return MongoClient.this.getDatabase(databaseName);
            }

            @Override
            public void close() {
                MongoClient.this.close();
            }
        };
    }

    private <TResult> ChangeStreamIterable<TResult> createChangeStreamIterable(@Nullable final ClientSession clientSession,
                                                                               final List<? extends Bson> pipeline,
                                                                               final Class<TResult> resultClass) {
        MongoClientOptions clientOptions = getMongoClientOptions();
        return MongoIterables.changeStreamOf(clientSession, "admin",
                delegate.getCodecRegistry(), clientOptions.getReadPreference(), clientOptions.getReadConcern(),
                createOperationExecutor(), pipeline, resultClass, ChangeStreamLevel.CLIENT, clientOptions.getRetryReads());
    }


    static DBObjectCodec getCommandCodec() {
        return new DBObjectCodec(getDefaultCodecRegistry());
    }
}
