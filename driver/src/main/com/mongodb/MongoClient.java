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

import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.gridfs.codecs.GridFSFileCodecProvider;
import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.Closeable;
import java.util.List;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * <p>A MongoDB client with internal connection pooling. For most applications, you should have one MongoClient instance for the entire
 * JVM.
 * <p>The following are equivalent, and all connect to the local database running on the default port:</p>
 * <pre>
 * MongoClient mongoClient1 = new MongoClient();
 * MongoClient mongoClient1 = new MongoClient("localhost");
 * MongoClient mongoClient2 = new MongoClient("localhost", 27017);
 * MongoClient mongoClient4 = new MongoClient(new ServerAddress("localhost"));
 * MongoClient mongoClient5 = new MongoClient(new ServerAddress("localhost"), new MongoClientOptions.Builder().build());
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
 * <p>In general, users of this class will pick up all of the default options specified in {@code MongoClientOptions}.  In particular, note
 * that the default value of the connectionsPerHost option has been increased to 100 from the old default value of 10 used by the superseded
 * {@code Mongo} class.</p>
 *
 * @see ReadPreference#primary()
 * @see com.mongodb.WriteConcern#ACKNOWLEDGED
 * @see MongoClientOptions
 * @see MongoClientURI
 * @since 2.10.0
 */
public class MongoClient extends Mongo implements Closeable {

    private static final CodecRegistry DEFAULT_CODEC_REGISTRY =
            fromProviders(asList(new ValueCodecProvider(),
                    new DBRefCodecProvider(),
                    new DocumentCodecProvider(new DocumentToDBRefTransformer()),
                    new DBObjectCodecProvider(),
                    new BsonValueCodecProvider(),
                    new IterableCodecProvider(new DocumentToDBRefTransformer()),
                    new GeoJsonCodecProvider(),
                    new GridFSFileCodecProvider()));

    /**
     * Gets the default codec registry.  It includes the following providers:
     * <ul>
     * <li>{@link org.bson.codecs.ValueCodecProvider}</li>
     * <li>{@link org.bson.codecs.DocumentCodecProvider}</li>
     * <li>{@link com.mongodb.DBObjectCodecProvider}</li>
     * <li>{@link org.bson.codecs.BsonValueCodecProvider}</li>
     * <li>{@link com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider}</li>
     * </ul>
     *
     * @return the default codec registry
     * @see MongoClientOptions#getCodecRegistry()
     * @since 3.0
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return DEFAULT_CODEC_REGISTRY;
    }

    /**
     * Creates an instance based on a (single) mongodb node (localhost, default port).
     */
    public MongoClient() {
        this(new ServerAddress());
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node.
     *
     * @param host server to connect to in format host[:port]
     */
    public MongoClient(final String host) {
        this(new ServerAddress(host));
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node (default port).
     *
     * @param host    server to connect to in format host[:port]
     * @param options default query options
     */
    public MongoClient(final String host, final MongoClientOptions options) {
        this(new ServerAddress(host), options);
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node.
     *
     * @param host the database's host address
     * @param port the port on which the database is running
     */
    public MongoClient(final String host, final int port) {
        this(new ServerAddress(host, port));
    }

    /**
     * Creates a Mongo instance based on a (single) mongodb node
     *
     * @param addr the database address
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
     * @see com.mongodb.ServerAddress
     * @since 2.11.0
     */
    public MongoClient(final ServerAddress addr, final List<MongoCredential> credentialsList, final MongoClientOptions options) {
        super(addr, credentialsList, options);
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
     * <p>Creates an instance based on a list of replica set members or mongos servers. For a replica set it will discover all members.
     * For a list with a single seed, the driver will still discover all members of the replica set.  For a direct
     * connection to a replica set member, with no discovery, use the {@link #MongoClient(ServerAddress, List)}
     * constructor instead.</p>
     *
     * <p>When there is more than one server to choose from based on the type of request (read or write) and the read preference (if it's a
     * read request), the driver will randomly select a server to send a request. This applies to both replica sets and sharded clusters.
     * The servers to randomly select from are further limited by the local threshold.  See
     * {@link MongoClientOptions#getLocalThreshold()}</p>
     *
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param credentialsList the list of credentials used to authenticate all connections
     * @see MongoClientOptions#getLocalThreshold()
     * @since 2.11.0
     */
    public MongoClient(final List<ServerAddress> seeds, final List<MongoCredential> credentialsList) {
        this(seeds, credentialsList, new MongoClientOptions.Builder().build());
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
        super(seeds, options);
    }

    /**
     * <p>Creates an instance based on a list of replica set members or mongos servers. For a replica set it will discover all members.
     * For a list with a single seed, the driver will still discover all members of the replica set.  For a direct
     * connection to a replica set member, with no discovery, use the {@link #MongoClient(ServerAddress, List, MongoClientOptions)}
     * constructor instead.</p>
     *
     * <p>When there is more than one server to choose from based on the type of request (read or write) and the read preference (if it's a
     * read request), the driver will randomly select a server to send a request. This applies to both replica sets and sharded clusters.
     * The servers to randomly select from are further limited by the local threshold.  See
     * {@link MongoClientOptions#getLocalThreshold()}</p>
     *
     * @param seeds Put as many servers as you can in the list and the system will figure out the rest.  This can either be a list of mongod
     *              servers in the same replica set or a list of mongos servers in the same sharded cluster.
     * @param credentialsList the list of credentials used to authenticate all connections
     * @param options         the options
     * @see MongoClientOptions#getLocalThreshold()
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
     * @throws MongoException if theres a failure
     */
    public MongoClient(final MongoClientURI uri) {
        super(uri);
    }

    /**
     * Gets the options that this client uses to connect to server.
     *
     * <p>Note: {@link MongoClientOptions} is immutable.</p>
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
     * Get a list of the database names
     *
     * @mongodb.driver.manual reference/commands/listDatabases List Databases
     * @return an iterable containing all the names of all the databases
     * @since 3.0
     */
    public MongoIterable<String> listDatabaseNames() {
        return new ListDatabasesIterableImpl<BsonDocument>(BsonDocument.class, getDefaultCodecRegistry(),
                ReadPreference.primary(), createOperationExecutor()).map(new Function<BsonDocument, String>() {
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
        return new ListDatabasesIterableImpl<T>(clazz, getMongoClientOptions().getCodecRegistry(),
                ReadPreference.primary(), createOperationExecutor());
    }

    /**
     * @param databaseName the name of the database to retrieve
     * @return a {@code MongoDatabase} representing the specified database
     */
    public MongoDatabase getDatabase(final String databaseName) {
        MongoClientOptions clientOptions = getMongoClientOptions();
        return new MongoDatabaseImpl(databaseName, clientOptions.getCodecRegistry(), clientOptions.getReadPreference(),
                clientOptions.getWriteConcern(), clientOptions.getReadConcern(), createOperationExecutor());
    }

    static DBObjectCodec getCommandCodec() {
        return new DBObjectCodec(getDefaultCodecRegistry());
    }
}
