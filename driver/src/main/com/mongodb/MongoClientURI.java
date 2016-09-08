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

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * Represents a <a href="http://www.mongodb.org/display/DOCS/Connections">URI</a>
 * which can be used to create a MongoClient instance. The URI describes the hosts to
 * be used and options.
 * <p>The format of the URI is:
 * <pre>
 *   mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
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
 *          <li>{@code true}: the driver sends a getLastError command after every update to ensure that the update succeeded
 * (see also {@code w} and {@code wtimeoutMS}).</li>
 *          <li>{@code false}: the driver does not send a getLastError command after every update.</li>
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
 *          <li>The driver adds { w : wValue } to the getLastError command. Implies {@code safe=true}.</li>
 *          <li>wValue is typically a number, but can be any string in order to allow for specifications like
 * {@code "majority"}</li>
 *      </ul>
 *  </li>
 *  <li>{@code wtimeoutMS=ms}
 *      <ul>
 *          <li>The driver adds { wtimeout : ms } to the getlasterror command. Implies {@code safe=true}.</li>
 *          <li>Used in combination with {@code w}</li>
 *      </ul>
 *  </li>
 * </ul>
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
 * <li>{@code maxStalenessMS=ms}. The maximum staleness in milliseconds. For use with any non-primary read preference, the driver estimates
 * the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses, and selects only those secondaries
 * whose staleness is less than or equal to maxStalenessMS.  The default is 0, meaning there is no staleness check.
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
    public String getUsername() {
        return proxied.getUsername();
    }

    /**
     * Gets the password
     *
     * @return the password
     */
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
    public String getDatabase() {
        return proxied.getDatabase();
    }


    /**
     * Gets the collection name
     *
     * @return the collection name
     */
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
    public MongoCredential getCredentials() {
        if (proxied.getCredentialList().isEmpty()) {
            return null;
        } else {
            return proxied.getCredentialList().get(0);
        }
    }

    /**
     * Gets the options
     *
     * @return the MongoClientOptions based on this URI.
     */
    public MongoClientOptions getOptions() {
        if (proxied.getReadPreference() != null) {
            builder.readPreference(proxied.getReadPreference());
        }
        if (proxied.getReadConcern() != null) {
            builder.readConcern(proxied.getReadConcern());
        }
        if (proxied.getWriteConcern() != null) {
            builder.writeConcern(proxied.getWriteConcern());
        }
        if (proxied.getMaxConnectionPoolSize() != null) {
            builder.connectionsPerHost(proxied.getMaxConnectionPoolSize());
        }
        if (proxied.getMinConnectionPoolSize() != null) {
            builder.minConnectionsPerHost(proxied.getMinConnectionPoolSize());
        }
        if (proxied.getMaxWaitTime() != null) {
            builder.maxWaitTime(proxied.getMaxWaitTime());
        }
        if (proxied.getThreadsAllowedToBlockForConnectionMultiplier() != null) {
            builder.threadsAllowedToBlockForConnectionMultiplier(proxied.getThreadsAllowedToBlockForConnectionMultiplier());
        }
        if (proxied.getMaxConnectionIdleTime() != null) {
            builder.maxConnectionIdleTime(proxied.getMaxConnectionIdleTime());
        }
        if (proxied.getMaxConnectionLifeTime() != null) {
            builder.maxConnectionLifeTime(proxied.getMaxConnectionLifeTime());
        }
        if (proxied.getSocketTimeout() != null) {
            builder.socketTimeout(proxied.getSocketTimeout());
        }
        if (proxied.getConnectTimeout() != null) {
            builder.connectTimeout(proxied.getConnectTimeout());
        }
        if (proxied.getRequiredReplicaSetName() != null) {
            builder.requiredReplicaSetName(proxied.getRequiredReplicaSetName());
        }
        if (proxied.getSslEnabled() != null) {
            builder.sslEnabled(proxied.getSslEnabled());
        }
        if (proxied.getSslInvalidHostnameAllowed() != null) {
            builder.sslInvalidHostNameAllowed(proxied.getSslInvalidHostnameAllowed());
        }
        if (proxied.getServerSelectionTimeout() != null) {
            builder.serverSelectionTimeout(proxied.getServerSelectionTimeout());
        }
        if (proxied.getLocalThreshold() != null) {
            builder.localThreshold(proxied.getLocalThreshold());
        }
        if (proxied.getHeartbeatFrequency() != null) {
            builder.heartbeatFrequency(proxied.getHeartbeatFrequency());
        }
        if (proxied.getApplicationName() != null) {
            builder.applicationName(proxied.getApplicationName());
        }

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
        if (getDatabase() != null ? !getDatabase().equals(that.getDatabase()) : that.getDatabase() != null) {
            return false;
        }
        if (getCollection() != null ? !getCollection().equals(that.getCollection()) : that.getCollection() != null) {
            return false;
        }
        if (getCredentials() != null ? !getCredentials().equals(that.getCredentials()) : that.getCredentials() != null) {
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
        result = 31 * result + (getCredentials() != null ? getCredentials().hashCode() : 0);
        result = 31 * result + getHosts().hashCode();
        result = 31 * result + (getDatabase() != null ? getDatabase().hashCode() : 0);
        result = 31 * result + (getCollection() != null ? getCollection().hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return proxied.toString();
    }
}
