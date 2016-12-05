/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.binding.SingleServerBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.client.MongoDriverInformation;
import com.mongodb.connection.BufferProvider;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.Connection;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandEventMulticaster;
import com.mongodb.event.CommandListener;
import com.mongodb.internal.connection.PowerOfTwoBufferPool;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.management.JMXConnectionPoolListener;
import com.mongodb.operation.CurrentOpOperation;
import com.mongodb.operation.FsyncUnlockOperation;
import com.mongodb.operation.ListDatabasesOperation;
import com.mongodb.operation.OperationExecutor;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.selector.LatencyMinimizingServerSelector;
import com.mongodb.selector.ServerSelector;
import org.bson.BsonBoolean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.connection.ClusterType.REPLICA_SET;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * <p>A database connection with internal connection pooling. For most applications, you should have one Mongo instance for the entire
 * JVM.</p>
 *
 * <p>Note: This class has been superseded by {@code MongoClient}, and may be deprecated in a future release.</p>
 *
 * @see MongoClient
 * @see ReadPreference
 * @see WriteConcern
 */
@ThreadSafe
public class Mongo {
    static final String ADMIN_DATABASE_NAME = "admin";

    private final ConcurrentMap<String, DB> dbCache = new ConcurrentHashMap<String, DB>();

    private volatile WriteConcern writeConcern;
    private volatile ReadPreference readPreference;
    private final ReadConcern readConcern;

    private final MongoClientOptions options;
    private final List<MongoCredential> credentialsList;

    private final Bytes.OptionHolder optionHolder;

    private final Cluster cluster;
    private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();

    private final ConcurrentLinkedQueue<ServerCursorAndNamespace> orphanedCursors = new ConcurrentLinkedQueue<ServerCursorAndNamespace>();
    private final ExecutorService cursorCleaningService;

    /**
     * Creates a Mongo instance based on a (single) mongodb node (localhost, default port)
     *
     * @throws MongoException if there's a failure
     * @deprecated Replaced by {@link MongoClient#MongoClient()})
     */
    @Deprecated
    public Mongo() {
        this(new ServerAddress(), createLegacyOptions());
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (default port)
     *
     * @param host server to connect to
     * @deprecated Replaced by {@link MongoClient#MongoClient(String)}
     */
    @Deprecated
    public Mongo(final String host) {
        this(new ServerAddress(host), createLegacyOptions());
    }


    /**
     * Creates a Mongo instance based on a (single) mongodb node (default port)
     *
     * @param host    server to connect to
     * @param options default query options
     * @deprecated Replaced by {@link MongoClient#MongoClient(String, MongoClientOptions)}
     */
    @Deprecated
    public Mongo(final String host,
                 @SuppressWarnings("deprecation")
                 final MongoOptions options) {
        this(new ServerAddress(host), options.toClientOptions());
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node
     *
     * @param host the host address of the database
     * @param port the port on which the database is running
     * @deprecated Replaced by {@link MongoClient#MongoClient(String, int)}
     */
    @Deprecated
    public Mongo(final String host, final int port) {
        this(new ServerAddress(host, port), createLegacyOptions());
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node
     *
     * @param address the database address
     * @see com.mongodb.ServerAddress
     * @deprecated Replaced by {@link MongoClient#MongoClient(ServerAddress)}
     */
    @Deprecated
    public Mongo(final ServerAddress address) {
        this(address, createLegacyOptions());
    }

    /**
     * Creates a Mongo instance based on a (single) mongo node using a given ServerAddress
     *
     * @param address the database address
     * @param options default query options
     * @see com.mongodb.ServerAddress
     * @deprecated Replaced by {@link MongoClient#MongoClient(ServerAddress, MongoClientOptions)}
     */
    @Deprecated
    public Mongo(final ServerAddress address,
                 @SuppressWarnings("deprecation")
                 final MongoOptions options) {
        this(address, options.toClientOptions());
    }

    /**
     * <p>Creates a Mongo in paired mode. </p>
     *
     * <p>This will also work for a replica set and will find all members (the master will be used by default).</p>
     *
     * @param left  left side of the pair
     * @param right right side of the pair
     * @see com.mongodb.ServerAddress
     * @deprecated Please use {@link MongoClient#MongoClient(java.util.List)} instead.
     */
    @Deprecated
    public Mongo(final ServerAddress left, final ServerAddress right) {
        this(asList(left, right), createLegacyOptions());
    }

    /**
     * <p>Creates a Mongo connection in paired mode. </p>
     *
     * <p>This will also work for a replica set and will find all members (the master will be used by default).</p>
     *
     * @param left    left side of the pair
     * @param right   right side of the pair
     * @param options the optional settings for the Mongo instance
     * @see com.mongodb.ServerAddress
     * @deprecated Please use {@link MongoClient#MongoClient(java.util.List, MongoClientOptions)} instead.
     */
    @Deprecated
    public Mongo(final ServerAddress left, final ServerAddress right,
                 @SuppressWarnings("deprecation")
                 final MongoOptions options) {
        this(asList(left, right), options.toClientOptions());
    }

    /**
     * <p>Creates an instance based on a list of replica set members or mongos servers. For a replica set it will discover all members.
     * For a list with a single seed, the driver will still discover all members of the replica set.  For a direct
     * connection to a replica set member, with no discovery, use the {@link #Mongo(ServerAddress)} constructor instead.</p>
     *
     * <p>When there is more than one server to choose from based on the type of request (read or write) and the read preference (if it's a
     * read request), the driver will randomly select a server to send a request. This applies to both replica sets and sharded clusters.
     * The servers to randomly select from are further limited by the local threshold.  See
     * {@link MongoClientOptions#getLocalThreshold()}</p>
     *
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @see MongoClientOptions#getLocalThreshold()
     * @deprecated Replaced by {@link MongoClient#MongoClient(java.util.List)}
     */
    @Deprecated
    public Mongo(final List<ServerAddress> seeds) {
        this(seeds, createLegacyOptions());
    }

    /**
     * <p>Creates an instance based on a list of replica set members or mongos servers. For a replica set it will discover all members.
     * For a list with a single seed, the driver will still discover all members of the replica set.  For a direct
     * connection to a replica set member, with no discovery, use the {@link #Mongo(ServerAddress, MongoClientOptions)} constructor
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
     * @deprecated Replaced by {@link MongoClient#MongoClient(java.util.List, MongoClientOptions)}
     */
    @Deprecated
    public Mongo(final List<ServerAddress> seeds,
                 @SuppressWarnings("deprecation")
                 final MongoOptions options) {
        this(seeds, options.toClientOptions());
    }

    /**
     * <p>Creates a Mongo described by a URI. If only one address is used it will only connect to that node, otherwise it will discover all
     * nodes. If the URI contains database credentials, the database will be authenticated lazily on first use with those credentials.</p>
     *
     * <p>Examples:</p>
     * <ul>
     *     <li>mongodb://localhost</li>
     *     <li>mongodb://fred:foobar@localhost/</li>
     * </ul>
     *
     * @param uri URI to connect to, optionally containing additional information like credentials
     * @throws MongoException if there's a failure
     * @mongodb.driver.manual reference/connection-string Connection String URI Format
     * @see MongoURI
     * @deprecated Replaced by {@link MongoClient#MongoClient(MongoClientURI)}
     */
    @Deprecated
    public Mongo(
                @SuppressWarnings("deprecation")
                final MongoURI uri) {
        this(uri.toClientURI());
    }

    Mongo(final List<ServerAddress> seedList, final MongoClientOptions options) {
        this(seedList, Collections.<MongoCredential>emptyList(), options);
    }

    Mongo(final ServerAddress serverAddress, final MongoClientOptions options) {
        this(serverAddress, Collections.<MongoCredential>emptyList(), options);
    }

    Mongo(final ServerAddress serverAddress, final List<MongoCredential> credentialsList, final MongoClientOptions options) {
        this(serverAddress, credentialsList, options, null);
    }

    Mongo(final ServerAddress serverAddress, final List<MongoCredential> credentialsList, final MongoClientOptions options,
          final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(serverAddress, credentialsList, options, mongoDriverInformation), options, credentialsList);
    }

    Mongo(final List<ServerAddress> seedList, final List<MongoCredential> credentialsList, final MongoClientOptions options) {
        this(seedList, credentialsList, options, null);
    }

    Mongo(final List<ServerAddress> seedList, final List<MongoCredential> credentialsList, final MongoClientOptions options,
          final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(seedList, credentialsList, options, mongoDriverInformation), options, credentialsList);
    }

    Mongo(final MongoClientURI mongoURI) {
        this(mongoURI, null);
    }

    Mongo(final MongoClientURI mongoURI, final MongoDriverInformation mongoDriverInformation) {
        this(createCluster(mongoURI, mongoDriverInformation), mongoURI.getOptions(),
                mongoURI.getCredentials() != null ? asList(mongoURI.getCredentials()) : Collections.<MongoCredential>emptyList());
    }

    Mongo(final Cluster cluster, final MongoClientOptions options, final List<MongoCredential> credentialsList) {
        this.cluster = cluster;
        this.options = options;
        this.readPreference = options.getReadPreference() != null ? options.getReadPreference() : primary();
        this.writeConcern = options.getWriteConcern() != null ? options.getWriteConcern() : WriteConcern.UNACKNOWLEDGED;
        this.readConcern = options.getReadConcern() != null ? options.getReadConcern() : ReadConcern.DEFAULT;
        this.optionHolder = new Bytes.OptionHolder(null);
        this.credentialsList = unmodifiableList(credentialsList);
        cursorCleaningService = options.isCursorFinalizerEnabled() ? createCursorCleaningService() : null;
    }

    /**
     * Sets the default write concern to use for write operations executed on any {@link DBCollection} created indirectly from this
     * instance, via a {@link DB} instance created from {@link #getDB(String)}.
     *
     * <p>
     *     Note that changes to the default write concern made via this method will NOT affect the write concern of
     *     {@link com.mongodb.client.MongoDatabase} instances created via {@link MongoClient#getDatabase(String)}
     * </p>
     *
     * @param writeConcern write concern to use
     * @deprecated Set the default write concern with either {@link MongoClientURI} or {@link MongoClientOptions}
     */
    @Deprecated
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    /**
     * Gets the write concern
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets the read concern
     *
     * @return the read concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Sets the default read preference to use for reads operations executed on any {@link DBCollection} created indirectly from this
     * instance, via a {@link DB} instance created from {@link #getDB(String)}.
     *
     * <p>
     *     Note that changes to the default read preference made via this method will NOT affect the read preference of
     *     {@link com.mongodb.client.MongoDatabase} instances created via {@link MongoClient#getDatabase(String)}
     * </p>

     * @param readPreference Read Preference to use
     * @deprecated Set the default read preference with either {@link MongoClientURI} or {@link MongoClientOptions}
     */
    @Deprecated
    public void setReadPreference(final ReadPreference readPreference) {
        this.readPreference = readPreference;
    }

    /**
     * Gets the default read preference
     *
     * @return the default read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets a list of all server addresses used when this Mongo was created
     *
     * @return list of server addresses
     * @throws MongoException if there's a failure
     */
    public List<ServerAddress> getAllAddress() {
        return cluster.getSettings().getHosts();
    }

    /**
     * Gets the list of server addresses currently seen by this client. This includes addresses auto-discovered from a replica set.
     *
     * @return list of server addresses
     * @throws MongoException if there's a failure
     */
    public List<ServerAddress> getServerAddressList() {
        List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
        for (final ServerDescription cur : getClusterDescription().getServerDescriptions()) {
            serverAddresses.add(cur.getAddress());
        }
        return serverAddresses;
    }

    private ClusterDescription getClusterDescription() {
        return cluster.getDescription();
    }

    /**
     * Gets the address of the current master
     *
     * @return the address
     */
    @SuppressWarnings("deprecation")
    public ServerAddress getAddress() {
        ClusterDescription description = getClusterDescription();
        if (description.getPrimaries().isEmpty()) {
            return null;
        }
        return description.getPrimaries().get(0).getAddress();
    }

    /**
     * <p>Returns the mongo options.</p>
     *
     * <p>Changes to {@code MongoOptions} that are done after connection are not reflected.</p>
     *
     * @return the mongo options
     * @deprecated Please use {@link MongoClient} class to connect to server and corresponding {@link
     * com.mongodb.MongoClient#getMongoClientOptions()}
     */
    @Deprecated
    public MongoOptions getMongoOptions() {
        return new MongoOptions(getMongoClientOptions());
    }

    /**
     * Get the status of the replica set cluster.
     *
     * @return replica set status information
     */
    public ReplicaSetStatus getReplicaSetStatus() {
        ClusterDescription clusterDescription = getClusterDescription();
        return clusterDescription.getType() == REPLICA_SET && clusterDescription.getConnectionMode() == MULTIPLE
               ? new ReplicaSetStatus(cluster) : null; // this is intended behavior in 2.x
    }


    /**
     * Gets a list of the names of all databases on the connected server.
     *
     * @return list of database names
     * @throws MongoException  if the operation fails
     * @deprecated Replaced with {@link com.mongodb.MongoClient#listDatabaseNames()}
     */
    @Deprecated
    public List<String> getDatabaseNames() {
      return new OperationIterable<DBObject>(new ListDatabasesOperation<DBObject>(MongoClient.getCommandCodec()),
          primary(), createOperationExecutor())
          .map(new Function<DBObject, String>() {
              @Override
              public String apply(final DBObject result) {
                  return (String) result.get("name");
              }
          }).into(new ArrayList<String>());
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
     * @deprecated use {@link com.mongodb.MongoClient#getDatabase(String)}
     */
    @Deprecated
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
     * Returns the list of databases used by the driver since this Mongo instance was created. This may include DBs that exist in the client
     * but not yet on the server.
     *
     * @return a collection of database objects
     */
    public Collection<DB> getUsedDatabases() {
        return dbCache.values();
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
        cluster.close();
        if (cursorCleaningService != null) {
            cursorCleaningService.shutdownNow();
        }
    }

    /**
     * Makes it possible to run read queries on secondary nodes
     *
     * @see ReadPreference#secondaryPreferred()
     * @deprecated Replaced with {@code ReadPreference.secondaryPreferred()}
     */
    @Deprecated
    public void slaveOk() {
        addOption(Bytes.QUERYOPTION_SLAVEOK);
    }

    /**
     * Set the default query options for reads operations executed on any {@link DBCollection} created indirectly from this
     * instance, via a {@link DB} instance created from {@link #getDB(String)}.
     *
     * <p>
     *     Note that changes to query options made via this method will NOT affect
     *     {@link com.mongodb.client.MongoDatabase} instances created via {@link MongoClient#getDatabase(String)}
     * </p>
     *
     * @param options value to be set
     * @deprecated Set options on instances of {@link DBCursor}
     */
    @Deprecated
    public void setOptions(final int options) {
        optionHolder.set(options);
    }

    /**
     * Reset the default query options for reads operations executed on any {@link DBCollection} created indirectly from this
     * instance, via a {@link DB} instance created from {@link #getDB(String)}.
     *
     * @deprecated Reset options instead on instances of {@link DBCursor}
     */
    @Deprecated
    public void resetOptions() {
        optionHolder.reset();
    }

    /**
     * Add the default query option for reads operations executed on any {@link DBCollection} created indirectly from this
     * instance, via a {@link DB} instance created from {@link #getDB(String)}.
     *
     * <p>
     *     Note that changes to query options made via this method will NOT affect
     *     {@link com.mongodb.client.MongoDatabase} instances created via {@link MongoClient#getDatabase(String)}
     * </p>
     *
     * @param option value to be added to current options
     * @deprecated Add options instead on instances of {@link DBCursor}
     */
    @Deprecated
    public void addOption(final int option) {
        optionHolder.add(option);
    }

    /**
     * Gets the default query options for reads operations executed on any {@link DBCollection} created indirectly from this
     * instance, via a {@link DB} instance created from {@link #getDB(String)}.
     *
     * @return an int representing the options to be used by queries
     * @deprecated Get options instead from instances of {@link DBCursor}
     */
    @Deprecated
    public int getOptions() {
        return optionHolder.get();
    }

    /**
     * Forces the master server to fsync the RAM data to disk This is done automatically by the server at intervals, but can be forced for
     * better reliability.
     *
     * @param async if true, the fsync will be done asynchronously on the server.
     * @return result of the command execution
     * @throws MongoException if there's a failure
     * @mongodb.driver.manual reference/command/fsync/ fsync command
     */
    public CommandResult fsync(final boolean async) {
        DBObject command = new BasicDBObject("fsync", 1);
        if (async) {
            command.put("async", 1);
        }
        return getDB(ADMIN_DATABASE_NAME).command(command);
    }

    /**
     * Forces the master server to fsync the RAM data to disk, then lock all writes. The database will be read-only after this command
     * returns.
     *
     * @return result of the command execution
     * @throws MongoException if there's a failure
     * @mongodb.driver.manual reference/command/fsync/ fsync command
     */
    public CommandResult fsyncAndLock() {
        DBObject command = new BasicDBObject("fsync", 1);
        command.put("lock", 1);
        return getDB(ADMIN_DATABASE_NAME).command(command);
    }

    /**
     * Unlocks the database, allowing the write operations to go through. This command may be asynchronous on the server, which means there
     * may be a small delay before the database becomes writable.
     *
     * @return {@code DBObject} in the following form {@code {"ok": 1,"info": "unlock completed"}}
     * @throws MongoException if there's a failure
     * @mongodb.driver.manual reference/command/fsync/ fsync command
     */
    public DBObject unlock() {
        return DBObjects.toDBObject(execute(new FsyncUnlockOperation()));
    }

    /**
     * Returns true if the database is locked (read-only), false otherwise.
     *
     * @return result of the command execution
     * @throws MongoException if the operation fails
     * @mongodb.driver.manual reference/command/fsync/ fsync command
     */
    public boolean isLocked() {
        return execute(new CurrentOpOperation(), ReadPreference.primary())
               .getBoolean("fsyncLock", BsonBoolean.FALSE).getValue();
    }

    @Override
    public String toString() {
        return "Mongo{"
               + "options=" + getMongoClientOptions()
               + '}';
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server. Note that this value may change over time depending
     * on which server is master.
     *
     * @return the maximum size, or 0 if not obtained from servers yet.
     * @throws MongoException if there's a failure
     */
    @SuppressWarnings("deprecation")
    public int getMaxBsonObjectSize() {
        List<ServerDescription> primaries = getClusterDescription().getPrimaries();
        return primaries.isEmpty() ? ServerDescription.getDefaultMaxDocumentSize() : primaries.get(0).getMaxDocumentSize();
    }

    /**
     * Gets a {@code String} representation of current connection point, i.e. master.
     *
     * @return server address in a host:port form
     */
    public String getConnectPoint() {
        ServerAddress master = getAddress();
        return master != null ? String.format("%s:%d", master.getHost(), master.getPort()) : null;
    }

    private static MongoClientOptions createLegacyOptions() {
        return MongoClientOptions.builder()
                                 .legacyDefaults()
                                 .build();
    }

    private static Cluster createCluster(final MongoClientURI mongoURI, final MongoDriverInformation mongoDriverInformation) {

        List<MongoCredential> credentialList = mongoURI.getCredentials() != null
                                               ? asList(mongoURI.getCredentials())
                                               : Collections.<MongoCredential>emptyList();

        if (mongoURI.getHosts().size() == 1) {
            return createCluster(new ServerAddress(mongoURI.getHosts().get(0)),
                                 credentialList,
                                 mongoURI.getOptions(), null);
        } else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>(mongoURI.getHosts().size());
            for (final String host : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(host));
            }
            return createCluster(seedList, credentialList, mongoURI.getOptions(), mongoDriverInformation);
        }
    }

    private static Cluster createCluster(final List<ServerAddress> seedList,
                                         final List<MongoCredential> credentialsList, final MongoClientOptions options,
                                         final MongoDriverInformation mongoDriverInformation) {
        return createCluster(ClusterSettings.builder().hosts(createNewSeedList(seedList))
                                            .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                            .serverSelectionTimeout(options.getServerSelectionTimeout(), MILLISECONDS)
                                            .serverSelector(createServerSelector(options))
                                            .description(options.getDescription())
                                            .maxWaitQueueSize(options.getConnectionPoolSettings().getMaxWaitQueueSize()),
                             credentialsList, options, mongoDriverInformation);
    }

    private static Cluster createCluster(final ServerAddress serverAddress, final List<MongoCredential> credentialsList,
                                         final MongoClientOptions options, final MongoDriverInformation mongoDriverInformation) {
        return createCluster(ClusterSettings.builder()
                                            .mode(getSingleServerClusterMode(options))
                                            .hosts(asList(serverAddress))
                                            .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                            .serverSelectionTimeout(options.getServerSelectionTimeout(), MILLISECONDS)
                                            .serverSelector(createServerSelector(options))
                                            .description(options.getDescription())
                                            .maxWaitQueueSize(options.getConnectionPoolSettings().getMaxWaitQueueSize()),
                             credentialsList, options, mongoDriverInformation);
    }

    private static Cluster createCluster(final ClusterSettings.Builder settingsBuilder, final List<MongoCredential> credentialsList,
                                         final MongoClientOptions options, final MongoDriverInformation mongoDriverInformation) {
        for (ClusterListener cur : options.getClusterListeners()) {
            settingsBuilder.addClusterListener(cur);
        }
        return new DefaultClusterFactory().create(settingsBuilder.build(),
                                                  options.getServerSettings(),
                                                  options.getConnectionPoolSettings(),
                                                  new SocketStreamFactory(options.getSocketSettings(),
                                                                          options.getSslSettings(),
                                                                          options.getSocketFactory()),
                                                  new SocketStreamFactory(options.getHeartbeatSocketSettings(),
                                                                          options.getSslSettings(),
                                                                          options.getSocketFactory()),
                                                  credentialsList, null,
                                                  new JMXConnectionPoolListener(), null,
                                                  createCommandListener(options.getCommandListeners()),
                                                  options.getApplicationName(),
                                                  mongoDriverInformation);
    }

    private static CommandListener createCommandListener(final List<CommandListener> commandListeners) {
        switch (commandListeners.size()) {
            case 0:
                return null;
            case 1:
                return commandListeners.get(0);
            default:
                return new CommandEventMulticaster(commandListeners);
        }
    }

    private static List<ServerAddress> createNewSeedList(final List<ServerAddress> seedList) {
        List<ServerAddress> retVal = new ArrayList<ServerAddress>(seedList.size());
        for (final ServerAddress cur : seedList) {
            retVal.add(cur);
        }
        return retVal;
    }

    private static ServerSelector createServerSelector(final MongoClientOptions options) {
        return new LatencyMinimizingServerSelector(options.getLocalThreshold(), MILLISECONDS);
    }

    Cluster getCluster() {
        return cluster;
    }

    Bytes.OptionHolder getOptionHolder() {
        return optionHolder;
    }

    BufferProvider getBufferProvider() {
        return bufferProvider;
    }

    MongoClientOptions getMongoClientOptions() {
        return options;
    }

    List<MongoCredential> getCredentialsList() {
        return credentialsList;
    }

    WriteBinding getWriteBinding() {
        return getReadWriteBinding(primary());
    }

    ReadBinding getReadBinding(final ReadPreference readPreference) {
        return getReadWriteBinding(readPreference);
    }

    private ReadWriteBinding getReadWriteBinding(final ReadPreference readPreference) {
        return new ClusterBinding(getCluster(), readPreference);
    }

    void addOrphanedCursor(final ServerCursor serverCursor, final MongoNamespace namespace) {
        orphanedCursors.add(new ServerCursorAndNamespace(serverCursor, namespace));
    }

    OperationExecutor createOperationExecutor() {
        return new OperationExecutor() {
            @Override
            public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
                return Mongo.this.execute(operation, readPreference);
            }

            @Override
            public <T> T execute(final WriteOperation<T> operation) {
                return Mongo.this.execute(operation);
            }
        };
    }

    <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
        ReadBinding binding = getReadBinding(readPreference);
        try {
            return operation.execute(binding);
        } finally {
            binding.release();
        }
    }

    <T> T execute(final WriteOperation<T> operation) {
        WriteBinding binding = getWriteBinding();
        try {
            return operation.execute(binding);
        } finally {
            binding.release();
        }
    }

    private ExecutorService createCursorCleaningService() {
        ScheduledExecutorService newTimer = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
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
            ReadWriteBinding binding = new SingleServerBinding(cluster, cur.serverCursor.getAddress());
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

        public ServerCursorAndNamespace(final ServerCursor serverCursor, final MongoNamespace namespace) {
            this.serverCursor = serverCursor;
            this.namespace = namespace;
        }
    }

    /**
     * Mongo.Holder can be used as a static place to hold several instances of Mongo. Security is not enforced at this level, and needs to
     * be done on the application side.
     */
    public static class Holder {

        private static final Holder INSTANCE = new Holder();
        private final ConcurrentMap<String, Mongo> clients = new ConcurrentHashMap<String, Mongo>();

        /**
         * Get the only instance of {@code Holder}.
         *
         * @return the singleton instance of {@code Holder}
         */
        public static Holder singleton() {
            return INSTANCE;
        }

        /**
         * Attempts to find an existing MongoClient instance matching that URI in the holder, and returns it if exists. Otherwise creates a
         * new Mongo instance based on this URI and adds it to the holder.
         *
         * @param uri the Mongo URI
         * @return the client
         * @throws MongoException if there's a failure
         * @deprecated Please use {@link #connect(MongoClientURI)} instead.
         */
        @Deprecated
        public Mongo connect(final MongoURI uri) {
            return connect(uri.toClientURI());
        }

        /**
         * Attempts to find an existing MongoClient instance matching that URI in the holder, and returns it if exists. Otherwise creates a
         * new Mongo instance based on this URI and adds it to the holder.
         *
         * @param uri the Mongo URI
         * @return the client
         * @throws MongoException if there's a failure
         */
        public Mongo connect(final MongoClientURI uri) {
            String key = toKey(uri);
            Mongo client = clients.get(key);

            if (client == null) {
                Mongo newbie = new MongoClient(uri);
                client = clients.putIfAbsent(key, newbie);
                if (client == null) {
                    client = newbie;
                } else {
                    newbie.close();
                }
            }

            return client;
        }

        private String toKey(final MongoClientURI uri) {
            return uri.toString();
        }
    }
}
