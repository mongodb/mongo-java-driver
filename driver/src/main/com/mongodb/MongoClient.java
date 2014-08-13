/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoDatabaseOptions;

import java.net.UnknownHostException;
import java.util.List;

/**
 * A MongoDB client with internal connection pooling. For most applications, you should have one MongoClient instance for the entire JVM.
 * <p/>
 * The following are equivalent, and all connect to the local database running on the default port:
 * <pre>
 * MongoClient mongoClient1 = new MongoClient();
 * MongoClient mongoClient1 = new MongoClient("localhost");
 * MongoClient mongoClient2 = new MongoClient("localhost", 27017);
 * MongoClient mongoClient4 = new MongoClient(new ServerAddress("localhost"));
 * MongoClient mongoClient5 = new MongoClient(new ServerAddress("localhost"), new MongoClientOptions.Builder().build());
 * </pre>
 * <p/>
 * You can connect to a <a href="http://www.mongodb.org/display/DOCS/Replica+Sets">replica set</a> using the Java driver by passing a
 * ServerAddress list to the MongoClient constructor. For example:
 * <pre>
 * MongoClient mongoClient = new MongoClient(Arrays.asList(
 *   new ServerAddress("localhost", 27017),
 *   new ServerAddress("localhost", 27018),
 *   new ServerAddress("localhost", 27019)));
 * </pre>
 * You can connect to a sharded cluster using the same constructor.  MongoClient will auto-detect whether the servers are a list of replica
 * set members or a list of mongos servers.
 * <p/>
 * By default, all read and write operations will be made on the primary, but it's possible to read from secondaries by changing the read
 * preference:
 * <pre>
 * mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
 * </pre>
 * By default, all write operations will wait for acknowledgment by the server, as the default write concern is {@code
 * WriteConcern.ACKNOWLEDGED}.
 * <p/>
 * Note: This class supersedes the {@code Mongo} class.  While it extends {@code Mongo}, it differs from it in that the default write
 * concern is to wait for acknowledgment from the server of all write operations.  In addition, its constructors accept instances of {@code
 * MongoClientOptions} and {@code MongoClientURI}, which both also set the same default write concern.
 * <p/>
 * In general, users of this class will pick up all of the default options specified in {@code MongoClientOptions}.  In particular, note
 * that the default value of the connectionsPerHost option has been increased to 100 from the old default value of 10 used by the superceded
 * {@code Mongo} class.
 *
 * @see ReadPreference#primary()
 * @see com.mongodb.WriteConcern#ACKNOWLEDGED
 * @see MongoClientOptions
 * @see MongoClientURI
 * @since 2.10.0
 */
public class MongoClient extends Mongo {

    /**
     * Creates an instance based on a (single) mongodb node (localhost, default port).
     *
     * @throws UnknownHostException
     * @throws MongoException
     */
    public MongoClient() throws UnknownHostException {
        this(new ServerAddress());
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node.
     *
     * @param host server to connect to in format host[:port]
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     */
    public MongoClient(final String host) throws UnknownHostException {
        this(new ServerAddress(host));
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (default port).
     *
     * @param host    server to connect to in format host[:port]
     * @param options default query options
     * @throws UnknownHostException if the database host cannot be resolved
     * @throws MongoException
     */
    public MongoClient(final String host, final MongoClientOptions options) throws UnknownHostException {
        this(new ServerAddress(host), options);
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node.
     *
     * @param host the database's host address
     * @param port the port on which the database is running
     * @throws MongoException
     */
    public MongoClient(final String host, final int port) {
        this(new ServerAddress(host, port));
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node
     *
     * @param addr the database address
     * @throws MongoException
     * @see com.mongodb.ServerAddress
     */
    public MongoClient(final ServerAddress addr) {
        this(addr, new MongoClientOptions.Builder().build());
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node and a list of credentials
     *
     * @param addr            the database address
     * @param credentialsList the list of credentials used to authenticate all connections
     * @throws MongoException
     * @see com.mongodb.ServerAddress
     * @since 2.11.0
     */
    public MongoClient(final ServerAddress addr, final List<MongoCredential> credentialsList) {
        this(addr, credentialsList, new MongoClientOptions.Builder().build());
    }

    /**
     * Creates a Mongo instance based on a (single) mongo node using a given ServerAddress and default options.
     *
     * @param addr    the database address
     * @param options default options
     * @throws MongoException
     * @see com.mongodb.ServerAddress
     */
    public MongoClient(final ServerAddress addr, final MongoClientOptions options) {
        super(addr, options);
    }

    /**
     * Creates a Mongo instance based on a (single) mongo node using a given ServerAddress and default options.
     *
     * @param addr            the database address
     * @param credentialsList the list of credentials used to authenticate all connections
     * @param options         default options
     * @throws MongoException
     * @see com.mongodb.ServerAddress
     * @since 2.11.0
     */
    public MongoClient(final ServerAddress addr, final List<MongoCredential> credentialsList, final MongoClientOptions options) {
        super(addr, credentialsList, options);
    }

    /**
     * Creates a Mongo based on a list of replica set members or a list of mongos. It will find all members (the master will be used by
     * default). If you pass in a single server in the list, the driver will still function as if it is a replica set. If you have a
     * standalone server, use the Mongo(ServerAddress) constructor.
     * <p/>
     * If this is a list of mongos servers, it will pick the closest (lowest ping time) one to send all requests to, and automatically fail
     * over to the next server if the closest is down.
     *
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @throws MongoException
     * @see com.mongodb.ServerAddress
     */
    public MongoClient(final List<ServerAddress> seeds) {
        this(seeds, new MongoClientOptions.Builder().build());
    }

    /**
     * Creates a Mongo based on a list of replica set members or a list of mongos. It will find all members (the master will be used by
     * default). If you pass in a single server in the list, the driver will still function as if it is a replica set. If you have a
     * standalone server, use the Mongo(ServerAddress) constructor.
     * <p/>
     * If this is a list of mongos servers, it will pick the closest (lowest ping time) one to send all requests to, and automatically fail
     * over to the next server if the closest is down.
     *
     * @param seeds           Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list
     *                        of mongod servers in the same replica set or a list of mongos servers in the same sharded cluster. \
     * @param credentialsList the list of credentials used to authenticate all connections
     * @throws MongoException
     * @see com.mongodb.ServerAddress
     * @since 2.11.0
     */
    public MongoClient(final List<ServerAddress> seeds, final List<MongoCredential> credentialsList) {
        this(seeds, credentialsList, new MongoClientOptions.Builder().build());
    }


    /**
     * Creates a Mongo based on a list of replica set members or a list of mongos. It will find all members (the master will be used by
     * default). If you pass in a single server in the list, the driver will still function as if it is a replica set. If you have a
     * standalone server, use the Mongo(ServerAddress) constructor.
     * <p/>
     * If this is a list of mongos servers, it will pick the closest (lowest ping time) one to send all reques        return new
     * MongoDatabaseImpl(databaseName, this, options.withDefaults(settings)); ts to, and automatically fail over to the next server if the
     * closest is down.
     *
     * @param seeds   Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of
     *                mongod servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param options default options
     * @throws MongoException
     * @see com.mongodb.ServerAddress
     */
    public MongoClient(final List<ServerAddress> seeds, final MongoClientOptions options) {
        super(seeds, options);
    }

    /**
     * Creates a Mongo based on a list of replica set members or a list of mongos. It will find all members (the master will be used by
     * default). If you pass in a single server in the list, the driver will still function as if it is a replica set. If you have a
     * standalone server, use the Mongo(ServerAddress) constructor.
     * <p/>
     * If this is a list of mongos servers, it will pick the closest (lowest ping time) one to send all requests to, and automatically fail
     * over to the next server if the closest is down.
     *
     * @param seeds           Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list
     *                        of mongod servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param credentialsList the list of credentials used to authenticate all connections
     * @param options         default options
     * @throws MongoException
     * @see com.mongodb.ServerAddress
     * @since 2.11.0
     */
    public MongoClient(final List<ServerAddress> seeds, final List<MongoCredential> credentialsList, final MongoClientOptions options) {
        super(seeds, credentialsList, options);
    }

    /**
     * Creates a Mongo described by a URI. If only one address is used it will only connect to that node, otherwise it will discover all
     * nodes.
     *
     * @param uri the URI
     * @throws MongoException
     * @throws UnknownHostException
     */
    public MongoClient(final MongoClientURI uri) throws UnknownHostException {
        super(uri);
    }

    /**
     * Gets the options that this client uses to connect to server. Please, note that {@link MongoClientOptions} is immutable.
     *
     * @return the options
     */
    public MongoClientOptions getMongoClientOptions() {
        return super.getMongoClientOptions();
    }

    /**
     * Gets the list of credentials that this client authenticates all connections with
     *
     * @return the list of credentials
     * @since 2.11.0
     */
    public List<MongoCredential> getCredentialsList() {
        return super.getCredentialsList();
    }

    /**
     * @param databaseName the name of the database to retrieve
     * @return a MongoDatabase representing the specified database
     */
    public MongoDatabase getDatabase(final String databaseName) {
        return new MongoDatabaseImpl(databaseName, this, MongoDatabaseOptions.builder()
                .writeConcern(getWriteConcern())
                .readPreference(getReadPreference())
                .codecRegistry(getMongoClientOptions().getCodecRegistry())
                .build());
    }

    /**
     * @param databaseName the name of the database to retrieve
     * @param options the database options
     * @return a MongoDatabse representing the specified database
     */
    public MongoDatabase getDatabase(final String databaseName, final MongoDatabaseOptions options) {
        return new MongoDatabaseImpl(databaseName, this, options);
    }
}
