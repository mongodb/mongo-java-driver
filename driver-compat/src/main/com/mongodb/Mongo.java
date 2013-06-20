/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.command.ListDatabases;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ClusterableServerFactory;
import org.mongodb.connection.ConnectionFactory;
import org.mongodb.connection.SSLSettings;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.impl.DefaultClusterFactory;
import org.mongodb.connection.impl.DefaultClusterableServerFactory;
import org.mongodb.connection.impl.DefaultConnectionFactory;
import org.mongodb.connection.impl.DefaultConnectionProviderFactory;
import org.mongodb.connection.impl.DefaultConnectionProviderSettings;
import org.mongodb.connection.impl.DefaultConnectionSettings;
import org.mongodb.connection.impl.DefaultServerSettings;
import org.mongodb.connection.impl.PowerOfTwoBufferPool;
import org.mongodb.session.ClusterSession;
import org.mongodb.session.PinnedSession;
import org.mongodb.session.ServerSelectingSession;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.connection.ClusterConnectionMode.Discovering;
import static org.mongodb.connection.ClusterType.ReplicaSet;

@ThreadSafe
public class Mongo {
    static final String ADMIN_DATABASE_NAME = "admin";
    private static final String VERSION = "3.0.0-SNAPSHOT";

    private final ConcurrentMap<String, DB> dbCache = new ConcurrentHashMap<String, DB>();

    private volatile WriteConcern writeConcern;
    private volatile ReadPreference readPreference;

    private final Bytes.OptionHolder optionHolder;

    private final Codec<Document> documentCodec;
    private final Cluster cluster;
    private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();

    private final ThreadLocal<ServerSelectingSession> pinnedSession = new ThreadLocal<ServerSelectingSession>();

    Mongo(final List<ServerAddress> seedList, final MongoClientOptions mongoOptions) {
        this(new DefaultClusterFactory().create(createNewSeedList(seedList), createClusterableServerFactory(mongoOptions.toNew())),
                mongoOptions);
    }

    Mongo(final MongoClientURI mongoURI) throws UnknownHostException {
        this(createCluster(mongoURI.toNew()), mongoURI.getOptions());
    }

    Mongo(final ServerAddress serverAddress, final MongoClientOptions mongoOptions) {
        this(new DefaultClusterFactory().create(serverAddress.toNew(), createClusterableServerFactory(mongoOptions.toNew())), mongoOptions);
    }

    public Mongo(final ServerAddress addr, final List<MongoCredential> credentialsList, final MongoClientOptions options) {
        this(new DefaultClusterFactory().create(addr.toNew(), createClusterableServerFactory(createNewCredentialList(credentialsList),
                options.toNew())), options);
    }

    Mongo(final List<ServerAddress> seedList, final List<MongoCredential> credentialsList, final MongoClientOptions options) {
        this(new DefaultClusterFactory().create(createNewSeedList(seedList),
                createClusterableServerFactory(createNewCredentialList(credentialsList), options.toNew())), options);
    }

    Mongo(final Cluster cluster, final MongoClientOptions options) {
        this.cluster = cluster;
        this.documentCodec = new DocumentCodec(PrimitiveCodecs.createDefault());
        this.readPreference = options.getReadPreference() != null ?
                options.getReadPreference() : ReadPreference.primary();
        this.writeConcern = options.getWriteConcern() != null ?
                options.getWriteConcern() : WriteConcern.UNACKNOWLEDGED;
        this.optionHolder = new Bytes.OptionHolder(null);
    }

    /**
     * Sets the write concern for this database. Will be used as default for writes to any collection in any database.
     * See the documentation for {@link WriteConcern} for more information.
     *
     * @param writeConcern write concern to use
     */
    public void setWriteConcern(final WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }


    /**
     * Gets the default write concern
     *
     * @return the default write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Sets the read preference for this database. Will be used as default for reads from any collection in any
     * database. See the documentation for {@link ReadPreference} for more information.
     *
     * @param readPreference Read Preference to use
     */
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
     * Gets the current driver version.
     *
     * @return the full version string, e.g. "3.0.0"
     */
    public static String getVersion() {
        return VERSION;
    }

    /**
     * Gets a list of all server addresses used when this Mongo was created
     *
     * @return list of server addresses
     * @throws MongoException
     */
    public List<ServerAddress> getAllAddress() {
        //TODO It should return the address list without auto-discovered nodes. Not sure if it's required. Maybe users confused with name.
        return getServerAddressList();
    }

    /**
     * Gets the list of server addresses currently seen by this client. This includes addresses auto-discovered from a
     * replica set.
     *
     * @return list of server addresses
     * @throws MongoException
     */
    public List<ServerAddress> getServerAddressList() {
        final List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
        for (final ServerDescription cur : cluster.getDescription().getAll()) {
            serverAddresses.add(new ServerAddress(cur.getAddress()));
        }
        return serverAddresses;
    }

    /**
     * Gets the address of the current master
     *
     * @return the address
     */
    public ServerAddress getAddress() {
        final ClusterDescription description = cluster.getDescription();
        if (description.getPrimaries().isEmpty()) {
            return null;
        }
        return new ServerAddress(description.getPrimaries().get(0).getAddress());
    }

    /**
     * Get the status of the replica set cluster.
     *
     * @return replica set status information
     */
    public ReplicaSetStatus getReplicaSetStatus() {
        return cluster.getDescription().getType() == ReplicaSet && cluster.getDescription().getMode() == Discovering
                ? new ReplicaSetStatus(cluster) : null; // this is intended behavior in 2.x
    }


    /**
     * Gets a list of the names of all databases on the connected server.
     *
     * @return list of database names
     * @throws MongoException
     */
    public List<String> getDatabaseNames() {
        //TODO: how do I make sure the exception is wrapped correctly?
        final org.mongodb.operation.CommandResult listDatabasesResult;
        listDatabasesResult = getDB(ADMIN_DATABASE_NAME).executeCommand(new ListDatabases());

        @SuppressWarnings("unchecked")
        final List<Document> databases = (List<Document>) listDatabasesResult.getResponse().get("databases");

        final List<String> databaseNames = new ArrayList<String>();
        for (final Document d : databases) {
            databaseNames.add(d.get("name", String.class));
        }
        return Collections.unmodifiableList(databaseNames);
    }

    /**
     * Gets a database object
     *
     * @param dbName the name of the database to retrieve
     * @return a DB representing the specified database
     */
    public DB getDB(final String dbName) {
        DB db = dbCache.get(dbName);
        if (db != null) {
            return db;
        }

        db = new DB(this, dbName, documentCodec);
        final DB temp = dbCache.putIfAbsent(dbName, db);
        if (temp != null) {
            return temp;
        }
        return db;
    }

    /**
     * Returns the list of databases used by the driver since this Mongo instance was created.
     * This may include DBs that exist in the client but not yet on the server.
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
     * @throws MongoException
     */
    public void dropDatabase(final String dbName) {
        getDB(dbName).dropDatabase();
    }

    /**
     * Closes all resources associated with this instance, in particular any open network connections. Once called, this
     * instance and any databases obtained from it can no longer be used.
     */
    public void close() {
        cluster.close();
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
     * Set the default query options.
     *
     * @param options value to be set
     */
    public void setOptions(final int options) {
        optionHolder.set(options);
    }

    /**
     * Reset the default query options.
     */
    public void resetOptions() {
        optionHolder.reset();
    }

    /**
     * Add the default query option.
     *
     * @param option value to be added to current options
     */
    public void addOption(final int option) {
        optionHolder.add(option);
    }

    public int getOptions() {
        return optionHolder.get();
    }

    private static List<org.mongodb.connection.ServerAddress> createNewSeedList(final List<ServerAddress> seedList) {
        final List<org.mongodb.connection.ServerAddress> retVal = new ArrayList<org.mongodb.connection.ServerAddress>(seedList.size());
        for (final ServerAddress cur : seedList) {
            retVal.add(cur.toNew());
        }
        return retVal;
    }

    private static List<org.mongodb.MongoCredential> createNewCredentialList(final List<MongoCredential> credentialsList) {
        if (credentialsList == null) {
            return Collections.emptyList();
        }
        final List<org.mongodb.MongoCredential> retVal = new ArrayList<org.mongodb.MongoCredential>(credentialsList.size());
        for (final MongoCredential cur : credentialsList) {
            retVal.add(cur.toNew());
        }
        return retVal;
    }

    private static Cluster createCluster(final org.mongodb.MongoClientURI mongoURI) throws UnknownHostException {
        if (mongoURI.getHosts().size() == 1) {
            return new DefaultClusterFactory().create(new org.mongodb.connection.ServerAddress(mongoURI.getHosts().get(0)),
                    createClusterableServerFactory(mongoURI.getCredentialList(), mongoURI.getOptions()));
        }
        else {
            final List<org.mongodb.connection.ServerAddress> seedList = new ArrayList<org.mongodb.connection.ServerAddress>();
            for (final String cur : mongoURI.getHosts()) {
                seedList.add(new org.mongodb.connection.ServerAddress(cur));
            }
            return new DefaultClusterFactory().create(seedList, createClusterableServerFactory(mongoURI.getCredentialList(),
                                                                                               mongoURI.getOptions()));
        }
    }

    private static ClusterableServerFactory createClusterableServerFactory(final org.mongodb.MongoClientOptions options) {
        return createClusterableServerFactory(Collections.<org.mongodb.MongoCredential>emptyList(), options);
    }

    private static ClusterableServerFactory createClusterableServerFactory(final List<org.mongodb.MongoCredential> credentialList,
                                                                           final org.mongodb.MongoClientOptions options) {
        final BufferProvider bufferProvider = new PowerOfTwoBufferPool();
        final SSLSettings sslSettings = SSLSettings.builder().enabled(options.isSSLEnabled()).build();

        final DefaultConnectionProviderSettings connectionProviderSettings = DefaultConnectionProviderSettings.builder()
                .maxSize(options.getConnectionsPerHost())
                .maxWaitQueueSize(options.getConnectionsPerHost() * options.getThreadsAllowedToBlockForConnectionMultiplier())
                .maxWaitTime(options.getMaxWaitTime(), TimeUnit.MILLISECONDS)
                .build();
        final DefaultConnectionSettings connectionSettings = DefaultConnectionSettings.builder()
                .connectTimeoutMS(options.getConnectTimeout())
                .readTimeoutMS(options.getSocketTimeout())
                .keepAlive(options.isSocketKeepAlive())
                .build();
        final ConnectionFactory connectionFactory = new DefaultConnectionFactory(connectionSettings, sslSettings, bufferProvider, credentialList);

        return new DefaultClusterableServerFactory(
                DefaultServerSettings.builder()
                        .heartbeatFrequency(Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000")),
                                TimeUnit.MILLISECONDS)
                        .build(),
                new DefaultConnectionProviderFactory(connectionProviderSettings, connectionFactory),
                null,
                new DefaultConnectionFactory(
                        DefaultConnectionSettings.builder()
                                .connectTimeoutMS(Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000")))
                                .readTimeoutMS(Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000")))
                                .keepAlive(options.isSocketKeepAlive())
                                .build(),
                        sslSettings, bufferProvider, credentialList),
                Executors.newScheduledThreadPool(3),  // TODO: allow configuration
                bufferProvider);
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

    ServerSelectingSession getSession() {
        if (pinnedSession.get() != null) {
            return pinnedSession.get();
        }
        return new ClusterSession(getCluster());
    }

    void pinSession() {
        isTrue("request not already started", pinnedSession.get() == null);
        pinnedSession.set(new PinnedSession(cluster));
    }

    void unpinSession() {
        isTrue("request started", pinnedSession.get() != null);
        final ServerSelectingSession sessionToUnpin = this.pinnedSession.get();
        this.pinnedSession.remove();
        sessionToUnpin.close();
    }
}
