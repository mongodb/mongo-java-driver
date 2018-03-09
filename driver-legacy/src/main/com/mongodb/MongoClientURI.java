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

import com.mongodb.lang.Nullable;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Represents a <a href="http://www.mongodb.org/display/DOCS/Connections">URI</a>
 * which can be used to create a MongoClient instance. The URI describes the hosts to
 * be used and options.
 * <p>The format of the URI is:
 * <pre>
 *   mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database.collection][?options]]
 * </pre>
 * <ul>
 * <li>{@code mongodb://} is a required prefix to identify that this is a string in the standard connection format.</li>
 * <li>{@code username:password@} are optional.  If given, the driver will attempt to login to a database after
 * connecting to a database server.  For some authentication mechanisms, only the username is specified and the password is not,
 * in which case the ":" after the username is left off as well</li>
 * <li>{@code host1} is the only required part of the URI.  It identifies a server address to connect to.</li>
 * <li>{@code :portX} is optional and defaults to :27017 if not provided.</li>
 * <li>{@code /database} is the name of the database to login to and thus is only relevant if the
 * {@code username:password@} syntax is used. If not specified the "admin" database will be used by default.</li>
 * <li>{@code ?options} are connection options. Note that if {@code database} is absent there is still a {@code /}
 * required between the last host and the {@code ?} introducing the options. Options are name=value pairs and the pairs
 * are separated by "&amp;". For backwards compatibility, ";" is accepted as a separator in addition to "&amp;",
 * but should be considered as deprecated.</li>
 * </ul>
 * <p>An alternative format, using the mongodb+srv protocol, is:
 * <pre>
 *   mongodb+srv://[username:password@]host[/[database][?options]]
 * </pre>
 * <ul>
 * <li>{@code mongodb+srv://} is a required prefix for this format.</li>
 * <li>{@code username:password@} are optional.  If given, the driver will attempt to login to a database after
 * connecting to a database server.  For some authentication mechanisms, only the username is specified and the password is not,
 * in which case the ":" after the username is left off as well</li>
 * <li>{@code host} is the only required part of the URI.  It identifies a single host name for which SRV records are looked up
 * from a Domain Name Server after prefixing the host name with {@code "_mongodb._tcp"}.  The host/port for each SRV record becomes the
 * seed list used to connect, as if each one were provided as host/port pair in a URI using the normal mongodb protocol.</li>
 * <li>{@code /database} is the name of the database to login to and thus is only relevant if the
 * {@code username:password@} syntax is used. If not specified the "admin" database will be used by default.</li>
 * <li>{@code ?options} are connection options. Note that if {@code database} is absent there is still a {@code /}
 * required between the last host and the {@code ?} introducing the options. Options are name=value pairs and the pairs
 * are separated by "&amp;". For backwards compatibility, ";" is accepted as a separator in addition to "&amp;",
 * but should be considered as deprecated. Additionally with the mongodb+srv protocol, TXT records are looked up from a Domain Name
 * Server for the given host, and the text value of each one is prepended to any options on the URI itself.  Because the last specified
 * value for any option wins, that means that options provided on the URI will override any that are provided via TXT records.</li>
 * </ul>
 * <p>The following options are supported (case insensitive):</p>
 *
 * <p>Server Selection Configuration:</p>
 * <ul>
 * <li>{@code serverSelectionTimeoutMS=ms}: How long the driver will wait for server selection to succeed before throwing an exception.</li>
 * <li>{@code localThresholdMS=ms}: When choosing among multiple MongoDB servers to send a request, the driver will only
 * send that request to a server whose ping time is less than or equal to the server with the fastest ping time plus the local
 * threshold.</li>
 * </ul>
 * <p>Server Monitoring Configuration:</p>
 * <ul>
 * <li>{@code heartbeatFrequencyMS=ms}: The frequency that the driver will attempt to determine the current state of each server in the
 * cluster.</li>
 * </ul>
 * <p>Replica set configuration:</p>
 * <ul>
 * <li>{@code replicaSet=name}: Implies that the hosts given are a seed list, and the driver will attempt to find
 * all members of the set.</li>
 * </ul>
 *
 * <p>Connection Configuration:</p>
 * <ul>
 * <li>{@code ssl=true|false}: Whether to connect using SSL.</li>
 * <li>{@code sslInvalidHostNameAllowed=true|false}: Whether to allow invalid host names for SSL connections.</li>
 * <li>{@code connectTimeoutMS=ms}: How long a connection can take to be opened before timing out.</li>
 * <li>{@code socketTimeoutMS=ms}: How long a send or receive on a socket can take before timing out.</li>
 * <li>{@code maxIdleTimeMS=ms}: Maximum idle time of a pooled connection. A connection that exceeds this limit will be closed</li>
 * <li>{@code maxLifeTimeMS=ms}: Maximum life time of a pooled connection. A connection that exceeds this limit will be closed</li>
 * </ul>
 *
 * <p>Connection pool configuration:</p>
 * <ul>
 * <li>{@code maxPoolSize=n}: The maximum number of connections in the connection pool.</li>
 * <li>{@code waitQueueMultiple=n} : this multiplier, multiplied with the maxPoolSize setting, gives the maximum number of
 * threads that may be waiting for a connection to become available from the pool.  All further threads will get an
 * exception right away.</li>
 * <li>{@code waitQueueTimeoutMS=ms}: The maximum wait time in milliseconds that a thread may wait for a connection to
 * become available.</li>
 * </ul>
 *
 * <p>Write concern configuration:</p>
 * <ul>
 *  <li>{@code safe=true|false}
 *      <ul>
 *          <li>{@code true}: the driver ensures that all writes are acknowledged by the MongoDB server, or else throws an exception.
 * (see also {@code w} and {@code wtimeoutMS}).</li>
 *          <li>{@code false}: the driver does not ensure that all writes are acknowledged by the MongoDB server.</li>
 *      </ul>
 *  </li>
 * <li>{@code journal=true|false}
 *  <ul>
 *      <li>{@code true}: the driver waits for the server to group commit to the journal file on disk.</li>
 *      <li>{@code false}: the driver does not wait for the server to group commit to the journal file on disk.</li>
 *  </ul>
 * </li>
 *  <li>{@code w=wValue}
 *      <ul>
 *          <li>The driver adds { w : wValue } to all write commands. Implies {@code safe=true}.</li>
 *          <li>wValue is typically a number, but can be any string in order to allow for specifications like
 * {@code "majority"}</li>
 *      </ul>
 *  </li>
 *  <li>{@code wtimeoutMS=ms}
 *      <ul>
 *          <li>The driver adds { wtimeout : ms } to all write commands. Implies {@code safe=true}.</li>
 *          <li>Used in combination with {@code w}</li>
 *      </ul>
 *  </li>
 *  <li>{@code retryWrites=true|false}. If true the driver will retry supported write operations if they fail due to a network error.
 *  Defaults to false.</li>
 * </ul>
 *
 *
 * <p>Read preference configuration:</p>
 * <ul>
 * <li>{@code slaveOk=true|false}: Whether a driver connected to a replica set will send reads to slaves/secondaries.</li>
 * <li>{@code readPreference=enum}: The read preference for this connection.  If set, it overrides any slaveOk value.
 * <ul>
 * <li>Enumerated values:
 * <ul>
 * <li>{@code primary}</li>
 * <li>{@code primaryPreferred}</li>
 * <li>{@code secondary}</li>
 * <li>{@code secondaryPreferred}</li>
 * <li>{@code nearest}</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * <li>{@code readPreferenceTags=string}.  A representation of a tag set as a comma-separated list of colon-separated
 * key-value pairs, e.g. {@code "dc:ny,rack:1}".  Spaces are stripped from beginning and end of all keys and values.
 * To specify a list of tag sets, using multiple readPreferenceTags,
 * e.g. {@code readPreferenceTags=dc:ny,rack:1;readPreferenceTags=dc:ny;readPreferenceTags=}
 * <ul>
 * <li>Note the empty value for the last one, which means match any secondary as a last resort.</li>
 * <li>Order matters when using multiple readPreferenceTags.</li>
 * </ul>
 * </li>
 * <li>{@code maxStalenessSeconds=seconds}. The maximum staleness in seconds. For use with any non-primary read preference, the driver
 * estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses, and selects only those
 * secondaries whose staleness is less than or equal to maxStalenessSeconds.  Not providing the parameter or explicitly setting it to -1
 * indicates that there should be no max staleness check.  The maximum staleness feature is designed to prevent badly-lagging servers from
 * being selected. The staleness estimate is imprecise and shouldn't be used to try to select "up-to-date" secondaries.  The minimum value
 * is either 90 seconds, or the heartbeat frequency plus 10 seconds, whichever is greatest.
 * </li>
 * </ul>
 * <p>Authentication configuration:</p>
 * <ul>
 * <li>{@code authMechanism=MONGO-CR|GSSAPI|PLAIN|MONGODB-X509}: The authentication mechanism to use if a credential was supplied.
 * The default is unspecified, in which case the client will pick the most secure mechanism available based on the sever version.  For the
 * GSSAPI and MONGODB-X509 mechanisms, no password is accepted, only the username.
 * </li>
 * <li>{@code authSource=string}: The source of the authentication credentials.  This is typically the database that
 * the credentials have been created.  The value defaults to the database specified in the path portion of the URI.
 * If the database is specified in neither place, the default value is "admin".  This option is only respected when using the MONGO-CR
 * mechanism (the default).
 * </li>
 * <li>{@code gssapiServiceName=string}: This option only applies to the GSSAPI mechanism and is used to alter the service name..
 * </li>
 * </ul>
 * <p>Server Handshake configuration:</p>
 * <ul>
 * <li>{@code appName=string}: Sets the logical name of the application.  The application name may be used by the client to identify
 * the application to the server, for use in server logs, slow query logs, and profile collection.</li>
 * </ul>
 * <p>Compressor configuration:</p>
 * <ul>
 * <li>{@code compressors=string}: A comma-separated list of compressors to request from the server.  The supported compressors
 * currently are 'zlib' and 'snappy'.</li>
 * <li>{@code zlibCompressionLevel=integer}: Integer value from -1 to 9 representing the zlib compression level. Lower values will make
 * compression faster, while higher values will make compression better.</li>
 * </ul>
 *
 * <p>Note: This class is a replacement for {@code MongoURI}, to be used with {@code MongoClient}.  The main difference in
 * behavior is that the default write concern is {@code WriteConcern.ACKNOWLEDGED}.</p>
 *
 * @mongodb.driver.manual reference/connection-string Connection String URI Format
 * @see MongoClientOptions for the default values for all options
 * @since 2.10.0
 */
public class MongoClientURI {
    private final ConnectionString proxied;
    private final MongoClientOptions.Builder builder;

    /**
     * Creates a MongoURI from the given string.
     *
     * @param uri the URI
     */
    public MongoClientURI(final String uri) {
        this(uri, new MongoClientOptions.Builder());
    }

    /**
     * Creates a MongoURI from the given URI string, and MongoClientOptions.Builder.  The builder can be configured with default options,
     * which may be overridden by options specified in the URI string.
     *
     * <p>
     * The {@code MongoClientURI} takes ownership of the {@code MongoClientOptions.Builder} instance that is passed to this constructor,
     * and may modify it.
     * </p>
     *
     * @param uri     the URI
     * @param builder a non-null Builder, which may be modified within this constructor,
     * @since 2.11.0
     */
    public MongoClientURI(final String uri, final MongoClientOptions.Builder builder) {
        this.builder = notNull("builder", builder);
        proxied = new ConnectionString(uri);
    }

    // ---------------------------------

    /**
     * Gets the username
     *
     * @return the username
     */
    @Nullable
    public String getUsername() {
        return proxied.getUsername();
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    @Nullable
    public char[] getPassword() {
        return proxied.getPassword();
    }

    /**
     * Gets the list of hosts
     *
     * @return the host list
     */
    public List<String> getHosts() {
        return proxied.getHosts();
    }

    /**
     * Gets the database name
     *
     * @return the database name
     */
    @Nullable
    public String getDatabase() {
        return proxied.getDatabase();
    }


    /**
     * Gets the collection name
     *
     * @return the collection name
     */
    @Nullable
    public String getCollection() {
        return proxied.getCollection();
    }

    /**
     * Get the unparsed URI.
     *
     * @return the URI
     */
    public String getURI() {
        return proxied.getConnectionString();
    }

    /**
     * Gets the credentials.
     *
     * @return the credentials
     */
    @Nullable
    public MongoCredential getCredentials() {
        return proxied.getCredential();
    }

    /**
     * Gets the options
     *
     * @return the MongoClientOptions based on this URI.
     */
    public MongoClientOptions getOptions() {
        ReadPreference readPreference = proxied.getReadPreference();
        if (readPreference != null) {
            builder.readPreference(readPreference);
        }
        ReadConcern readConcern = proxied.getReadConcern();
        if (readConcern != null) {
            builder.readConcern(readConcern);
        }
        WriteConcern writeConcern = proxied.getWriteConcern();
        if (writeConcern != null) {
            builder.writeConcern(writeConcern);
        }
        if (proxied.getRetryWrites()) {
            builder.retryWrites(proxied.getRetryWrites());
        }
        Integer maxConnectionPoolSize = proxied.getMaxConnectionPoolSize();
        if (maxConnectionPoolSize != null) {
            builder.connectionsPerHost(maxConnectionPoolSize);
        }
        Integer integer = proxied.getMinConnectionPoolSize();
        if (integer != null) {
            builder.minConnectionsPerHost(integer);
        }
        Integer maxWaitTime = proxied.getMaxWaitTime();
        if (maxWaitTime != null) {
            builder.maxWaitTime(maxWaitTime);
        }
        Integer threadsAllowedToBlockForConnectionMultiplier = proxied.getThreadsAllowedToBlockForConnectionMultiplier();
        if (threadsAllowedToBlockForConnectionMultiplier != null) {
            builder.threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier);
        }
        Integer maxConnectionIdleTime = proxied.getMaxConnectionIdleTime();
        if (maxConnectionIdleTime != null) {
            builder.maxConnectionIdleTime(maxConnectionIdleTime);
        }
        Integer maxConnectionLifeTime = proxied.getMaxConnectionLifeTime();
        if (maxConnectionLifeTime != null) {
            builder.maxConnectionLifeTime(maxConnectionLifeTime);
        }
        Integer socketTimeout = proxied.getSocketTimeout();
        if (socketTimeout != null) {
            builder.socketTimeout(socketTimeout);
        }
        Integer connectTimeout = proxied.getConnectTimeout();
        if (connectTimeout != null) {
            builder.connectTimeout(connectTimeout);
        }
        String requiredReplicaSetName = proxied.getRequiredReplicaSetName();
        if (requiredReplicaSetName != null) {
            builder.requiredReplicaSetName(requiredReplicaSetName);
        }
        Boolean sslEnabled = proxied.getSslEnabled();
        if (sslEnabled != null) {
            builder.sslEnabled(sslEnabled);
        }
        Boolean sslInvalidHostnameAllowed = proxied.getSslInvalidHostnameAllowed();
        if (sslInvalidHostnameAllowed != null) {
            builder.sslInvalidHostNameAllowed(sslInvalidHostnameAllowed);
        }
        Integer serverSelectionTimeout = proxied.getServerSelectionTimeout();
        if (serverSelectionTimeout != null) {
            builder.serverSelectionTimeout(serverSelectionTimeout);
        }
        Integer localThreshold = proxied.getLocalThreshold();
        if (localThreshold != null) {
            builder.localThreshold(localThreshold);
        }
        Integer heartbeatFrequency = proxied.getHeartbeatFrequency();
        if (heartbeatFrequency != null) {
            builder.heartbeatFrequency(heartbeatFrequency);
        }
        String applicationName = proxied.getApplicationName();
        if (applicationName != null) {
            builder.applicationName(applicationName);
        }
        builder.compressorList(proxied.getCompressorList());

        return builder.build();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MongoClientURI that = (MongoClientURI) o;

        if (!getHosts().equals(that.getHosts())) {
            return false;
        }
        String database = getDatabase();
        if (database != null ? !database.equals(that.getDatabase()) : that.getDatabase() != null) {
            return false;
        }
        String collection = getCollection();
        if (collection != null ? !collection.equals(that.getCollection()) : that.getCollection() != null) {
            return false;
        }
        MongoCredential credentials = getCredentials();
        if (credentials != null ? !credentials.equals(that.getCredentials()) : that.getCredentials() != null) {
            return false;
        }
        if (!getOptions().equals(that.getOptions())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getOptions().hashCode();
        result = 31 * result + getHosts().hashCode();

        MongoCredential credentials = getCredentials();
        result = 31 * result + (credentials != null ? credentials.hashCode() : 0);

        String database = getDatabase();
        result = 31 * result + (database != null ? database.hashCode() : 0);

        String collection = getCollection();
        result = 31 * result + (collection != null ? collection.hashCode() : 0);

        return result;
    }

    @Override
    public String toString() {
        return proxied.toString();
    }
}
