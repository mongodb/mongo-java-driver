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

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerMonitoringMode;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.event.ConnectionCheckOutStartedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.internal.connection.ServerMonitoringModeUtil;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.dns.DefaultDnsResolver;
import com.mongodb.lang.Nullable;
import com.mongodb.spi.dns.DnsClient;
import org.bson.UuidRepresentation;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.MongoCredential.ALLOWED_HOSTS_KEY;
import static com.mongodb.internal.connection.OidcAuthenticator.OidcValidator.validateCreateOidcCredential;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;


/**
 * <p>Represents a <a href="https://www.mongodb.com/docs/manual/reference/connection-string/">Connection String</a>.
 * The Connection String describes the hosts to be used and options.</p>
 *
 * <p>The format of the Connection String is:</p>
 * <pre>
 *   mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database.collection][?options]]
 * </pre>
 * <ul>
 * <li>{@code mongodb://} is a required prefix to identify that this is a string in the standard connection format.</li>
 * <li>{@code username:password@} are optional.  If given, the driver will attempt to login to a database after
 * connecting to a database server.  For some authentication mechanisms, only the username is specified and the password is not,
 * in which case the ":" after the username is left off as well</li>
 * <li>{@code host1} is the only required part of the connection string. It identifies a server address to connect to.
 * Support for Unix domain sockets was added in 3.7. Note: The path must be urlencoded eg: {@code mongodb://%2Ftmp%2Fmongodb-27017.sock}
 * and the {@code jnr.unixsocket} library installed.
 * </li>
 * <li>{@code :portX} is optional and defaults to :27017 if not provided.</li>
 * <li>{@code /database} is the name of the database to login to and thus is only relevant if the
 * {@code username:password@} syntax is used. If not specified the "admin" database will be used by default.</li>
 * <li>{@code ?options} are connection options. Options are name=value pairs and the pairs
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
 * from a Domain Name Server after prefixing the host name with, by default, {@code "_mongodb._tcp"} ({@code "mongodb"} is the default SRV
 * service name, but can be replaced via the {@code srvServiceName} query parameter),  The host/port for each SRV record becomes the
 * seed list used to connect, as if each one were provided as host/port pair in a URI using the normal mongodb protocol.</li>
 * <li>{@code /database} is the name of the database to login to and thus is only relevant if the
 * {@code username:password@} syntax is used. If not specified the "admin" database will be used by default.</li>
 * <li>{@code ?options} are connection options. Options are name=value pairs and the pairs
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
 * <li>{@code serverMonitoringMode=enum}: The server monitoring mode, which defines the monitoring protocol to use. Enumerated values:
 *  <ul>
 *      <li>{@code stream};</li>
 *      <li>{@code poll};</li>
 *      <li>{@code auto} - the default.</li>
 *  </ul>
 * </li>
 * </ul>
 * <p>Replica set configuration:</p>
 * <ul>
 * <li>{@code replicaSet=name}: Implies that the hosts given are a seed list, and the driver will attempt to find
 * all members of the set.</li>
 * </ul>
 * <p>Connection Configuration:</p>
 * <ul>
 * <li>{@code ssl=true|false}: Whether to connect using TLS.</li>
 * <li>{@code tls=true|false}: Whether to connect using TLS. Supersedes the ssl option</li>
 * <li>{@code tlsInsecure=true|false}: If connecting with TLS, this option enables insecure TLS connections. Currently this has the
 * same effect of setting tlsAllowInvalidHostnames to true. Other mechanism for relaxing TLS security constraints must be handled in
 * the application by customizing the {@link javax.net.ssl.SSLContext}</li>
 * <li>{@code sslInvalidHostNameAllowed=true|false}: Whether to allow invalid host names for TLS connections.</li>
 * <li>{@code tlsAllowInvalidHostnames=true|false}: Whether to allow invalid host names for TLS connections. Supersedes the
 * sslInvalidHostNameAllowed option</li>
 * <li>{@code timeoutMS=ms}: Time limit for the full execution of an operation. Note: This parameter is part of an {@linkplain Alpha Alpha API} and may be
 * subject to changes or even removal in future releases.</li>
 * <li>{@code connectTimeoutMS=ms}: How long a connection can take to be opened before timing out.</li>
 * <li>{@code socketTimeoutMS=ms}: How long a receive on a socket can take before timing out.
 * This option is the same as {@link SocketSettings#getReadTimeout(TimeUnit)}.
 * Deprecated, use {@code timeoutMS} instead.</li>
 * <li>{@code maxIdleTimeMS=ms}: Maximum idle time of a pooled connection. A connection that exceeds this limit will be closed</li>
 * <li>{@code maxLifeTimeMS=ms}: Maximum life time of a pooled connection. A connection that exceeds this limit will be closed</li>
 * </ul>
 * <p>Proxy Configuration:</p>
 * <ul>
 * <li>{@code proxyHost=string}: The SOCKS5 proxy host to establish a connection through.
 * It can be provided as a valid IPv4 address, IPv6 address, or a domain name. Required if either proxyPassword, proxyUsername or
 * proxyPort are specified</li>
 * <li>{@code proxyPort=n}: The port number for the SOCKS5 proxy server. Must be a non-negative integer.</li>
 * <li>{@code proxyUsername=string}: Username for authenticating with the proxy server. Required if proxyPassword is specified.</li>
 * <li>{@code proxyPassword=string}: Password for authenticating with the proxy server. Required if proxyUsername is specified.</li>
 * </ul>
 * <p>Connection pool configuration:</p>
 * <ul>
 * <li>{@code maxPoolSize=n}: The maximum number of connections in the connection pool.</li>
 * <li>{@code minPoolSize=n}: The minimum number of connections in the connection pool.</li>
 * <li>{@code waitQueueTimeoutMS=ms}: The maximum duration to wait until either:
 * an {@linkplain ConnectionCheckedOutEvent in-use connection} becomes {@linkplain ConnectionCheckedInEvent available},
 * or a {@linkplain ConnectionCreatedEvent connection is created} and begins to be {@linkplain ConnectionReadyEvent established}.
 * See {@link #getMaxWaitTime()} for more details. . Deprecated, use {@code timeoutMS} instead.</li>
 * <li>{@code maxConnecting=n}: The maximum number of connections a pool may be establishing concurrently.</li>
 * </ul>
 * <p>Write concern configuration:</p>
 * <ul>
 * <li>{@code safe=true|false}
 * <ul>
 * <li>{@code true}: the driver ensures that all writes are acknowledged by the MongoDB server, or else throws an exception.
 * (see also {@code w} and {@code wtimeoutMS}).</li>
 * <li>{@code false}: the driver does not ensure that all writes are acknowledged by the MongoDB server.</li>
 * </ul>
 * </li>
 * <li>{@code journal=true|false}
 * <ul>
 * <li>{@code true}: the driver waits for the server to group commit to the journal file on disk.</li>
 * <li>{@code false}: the driver does not wait for the server to group commit to the journal file on disk.</li>
 * </ul>
 * </li>
 * <li>{@code w=wValue}
 * <ul>
 * <li>The driver adds { w : wValue } to all write commands. Implies {@code safe=true}.</li>
 * <li>wValue is typically a number, but can be any string in order to allow for specifications like
 * {@code "majority"}</li>
 * </ul>
 * </li>
 * <li>{@code wtimeoutMS=ms}
 * <ul>
 * <li>The driver adds { wtimeout : ms } to all write commands. Implies {@code safe=true}.</li>
 * <li>Used in combination with {@code w}. Deprecated, use {@code timeoutMS} instead</li>
 * </ul>
 * </li>
 * </ul>
 * <p>Read preference configuration:</p>
 * <ul>
 * <li>{@code readPreference=enum}: The read preference for this connection.
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
 * estimates the staleness of each secondary, based on lastWriteDate values provided in server hello responses, and selects only those
 * secondaries whose staleness is less than or equal to maxStalenessSeconds.  Not providing the parameter or explicitly setting it to -1
 * indicates that there should be no max staleness check. The maximum staleness feature is designed to prevent badly-lagging servers from
 * being selected. The staleness estimate is imprecise and shouldn't be used to try to select "up-to-date" secondaries.  The minimum value
 * is either 90 seconds, or the heartbeat frequency plus 10 seconds, whichever is greatest.
 * </li>
 * </ul>
 * <p>Authentication configuration:</p>
 * <ul>
 * <li>{@code authMechanism=MONGO-CR|GSSAPI|PLAIN|MONGODB-X509|MONGODB-OIDC}: The authentication mechanism to use if a credential was supplied.
 * The default is unspecified, in which case the client will pick the most secure mechanism available based on the sever version.  For the
 * GSSAPI, MONGODB-X509, and MONGODB-OIDC mechanisms, no password is accepted, only the username.
 * </li>
 * <li>{@code authSource=string}: The source of the authentication credentials.  This is typically the database that
 * the credentials have been created.  The value defaults to the database specified in the path portion of the connection string.
 * If the database is specified in neither place, the default value is "admin".  This option is only respected when using the MONGO-CR
 * mechanism (the default).
 * </li>
 * <li>{@code authMechanismProperties=PROPERTY_NAME:PROPERTY_VALUE,PROPERTY_NAME2:PROPERTY_VALUE2}: This option allows authentication
 * mechanism properties to be set on the connection string.
 * </li>
 * <li>{@code gssapiServiceName=string}: This option only applies to the GSSAPI mechanism and is used to alter the service name.
 *   Deprecated, please use {@code authMechanismProperties=SERVICE_NAME:string} instead.
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
 * currently are 'zlib', 'snappy' and 'zstd'.</li>
 * <li>{@code zlibCompressionLevel=integer}: Integer value from -1 to 9 representing the zlib compression level. Lower values will make
 * compression faster, while higher values will make compression better.</li>
 * </ul>
 * <p>SRV configuration:</p>
 * <ul>
 * <li>{@code srvServiceName=string}: The SRV service name. See {@link ClusterSettings#getSrvServiceName()} for details.</li>
 * <li>{@code srvMaxHosts=number}: The maximum number of hosts from the SRV record to connect to.</li>
 * </ul>
 * <p>General configuration:</p>
 * <ul>
 * <li>{@code retryWrites=true|false}. If true the driver will retry supported write operations if they fail due to a network error.
 *  Defaults to true.</li>
 * <li>{@code retryReads=true|false}. If true the driver will retry supported read operations if they fail due to a network error.
 *  Defaults to true.</li>
 * <li>{@code uuidRepresentation=unspecified|standard|javaLegacy|csharpLegacy|pythonLegacy}.  See
 * {@link MongoClientSettings#getUuidRepresentation()} for documentation of semantics of this parameter.  Defaults to "javaLegacy", but
 * will change to "unspecified" in the next major release.</li>
 * <li>{@code directConnection=true|false}. If true the driver will set the connection to be a direct connection to the host.</li>
 * <li>{@code loadBalanced=true|false}. If true the driver will assume that it's connecting to MongoDB through a load balancer.</li>
 * </ul>
 *
 * @mongodb.driver.manual reference/connection-string Connection String Format
 * @since 3.0.0
 */
public class ConnectionString {

    private static final String MONGODB_PREFIX = "mongodb://";
    private static final String MONGODB_SRV_PREFIX = "mongodb+srv://";
    private static final Set<String> ALLOWED_OPTIONS_IN_TXT_RECORD =
            new HashSet<>(asList("authsource", "replicaset", "loadbalanced"));
    private static final Logger LOGGER = Loggers.getLogger("uri");
    private static final List<String> MECHANISM_KEYS_DISALLOWED_IN_CONNECTION_STRING = Stream.of(ALLOWED_HOSTS_KEY)
            .map(k -> k.toLowerCase())
            .collect(Collectors.toList());

    private final MongoCredential credential;
    private final boolean isSrvProtocol;
    private final List<String> hosts;
    private final String database;
    private final String collection;
    private final String connectionString;

    private Integer srvMaxHosts;
    private String srvServiceName;
    private Boolean directConnection;
    private Boolean loadBalanced;
    private ReadPreference readPreference;
    private WriteConcern writeConcern;
    private Boolean retryWrites;
    private Boolean retryReads;
    private ReadConcern readConcern;

    private Integer minConnectionPoolSize;
    private Integer maxConnectionPoolSize;
    private Integer maxWaitTime;
    private Integer maxConnectionIdleTime;
    private Integer maxConnectionLifeTime;
    private Integer maxConnecting;
    private Integer connectTimeout;
    private Long timeout;
    private Integer socketTimeout;
    private Boolean sslEnabled;
    private Boolean sslInvalidHostnameAllowed;
    private String proxyHost;
    private Integer proxyPort;
    private String proxyUsername;
    private String proxyPassword;
    private String requiredReplicaSetName;
    private Integer serverSelectionTimeout;
    private Integer localThreshold;
    private Integer heartbeatFrequency;
    private ServerMonitoringMode serverMonitoringMode;
    private String applicationName;
    private List<MongoCompressor> compressorList;
    private UuidRepresentation uuidRepresentation;

    /**
     * Creates a ConnectionString from the given string.
     *
     * @param connectionString     the connection string
     * @since 3.0
     */
    public ConnectionString(final String connectionString) {
        this(connectionString, null);
    }

    /**
     * Creates a ConnectionString from the given string with the given {@link DnsClient}.
     *
     * <p>If setting {@link MongoClientSettings#getDnsClient()} explicitly, care should be taken to call this constructor with the same
     * {@link DnsClient}.
     *
     * @param connectionString the connection string
     * @param dnsClient        the DNS client with which to resolve TXT record for the mongodb+srv protocol
     * @since 4.10
     * @see MongoClientSettings#getDnsClient()
     */
    public ConnectionString(final String connectionString, @Nullable final DnsClient dnsClient) {
        this.connectionString = connectionString;
        boolean isMongoDBProtocol = connectionString.startsWith(MONGODB_PREFIX);
        isSrvProtocol = connectionString.startsWith(MONGODB_SRV_PREFIX);
        if (!isMongoDBProtocol && !isSrvProtocol) {
            throw new IllegalArgumentException(format("The connection string is invalid. "
                    + "Connection strings must start with either '%s' or '%s", MONGODB_PREFIX, MONGODB_SRV_PREFIX));
        }

        String unprocessedConnectionString;
        if (isMongoDBProtocol) {
            unprocessedConnectionString = connectionString.substring(MONGODB_PREFIX.length());
        } else {
            unprocessedConnectionString = connectionString.substring(MONGODB_SRV_PREFIX.length());
        }

        // Split out the user and host information
        String userAndHostInformation;
        int firstForwardSlashIdx = unprocessedConnectionString.indexOf("/");
        int firstQuestionMarkIdx = unprocessedConnectionString.indexOf("?");
        if (firstQuestionMarkIdx == -1 && firstForwardSlashIdx == -1) {
            userAndHostInformation = unprocessedConnectionString;
            unprocessedConnectionString = "";
        } else if (firstQuestionMarkIdx != -1 && (firstForwardSlashIdx == -1 || firstQuestionMarkIdx < firstForwardSlashIdx)) {
            // there is a question mark, and there is no slash or the question mark comes before any slash
            userAndHostInformation = unprocessedConnectionString.substring(0, firstQuestionMarkIdx);
            unprocessedConnectionString = unprocessedConnectionString.substring(firstQuestionMarkIdx);
        } else {
            userAndHostInformation = unprocessedConnectionString.substring(0, firstForwardSlashIdx);
            unprocessedConnectionString = unprocessedConnectionString.substring(firstForwardSlashIdx + 1);
        }

        // Split the user and host information
        String userInfo;
        String hostIdentifier;
        String userName = null;
        char[] password = null;
        int idx = userAndHostInformation.lastIndexOf("@");
        if (idx > 0) {
            userInfo = userAndHostInformation.substring(0, idx).replace("+", "%2B");
            hostIdentifier = userAndHostInformation.substring(idx + 1);
            int colonCount = countOccurrences(userInfo, ":");
            if (userInfo.contains("@") || colonCount > 1) {
                throw new IllegalArgumentException("The connection string contains invalid user information. "
                        + "If the username or password contains a colon (:) or an at-sign (@) then it must be urlencoded");
            }
            if (colonCount == 0) {
                userName = urldecode(userInfo);
            } else {
                idx = userInfo.indexOf(":");
                if (idx == 0) {
                    throw new IllegalArgumentException("No username is provided in the connection string");
                }
                userName = urldecode(userInfo.substring(0, idx));
                password = urldecode(userInfo.substring(idx + 1), true).toCharArray();
            }
        } else if (idx == 0) {
            throw new IllegalArgumentException("The connection string contains an at-sign (@) without a user name");
        } else {
            hostIdentifier = userAndHostInformation;
        }

        // Validate the hosts
        List<String> unresolvedHosts = unmodifiableList(parseHosts(asList(hostIdentifier.split(","))));
        if (isSrvProtocol) {
            if (unresolvedHosts.size() > 1) {
                throw new IllegalArgumentException("Only one host allowed when using mongodb+srv protocol");
            }
            if (unresolvedHosts.get(0).contains(":")) {
                throw new IllegalArgumentException("Host for when using mongodb+srv protocol can not contain a port");
            }
        }
        this.hosts = unresolvedHosts;

        // Process the authDB section
        String nsPart;
        idx = unprocessedConnectionString.indexOf("?");
        if (idx == -1) {
            nsPart = unprocessedConnectionString;
            unprocessedConnectionString = "";
        } else {
            nsPart = unprocessedConnectionString.substring(0, idx);
            unprocessedConnectionString = unprocessedConnectionString.substring(idx + 1);
        }
        if (nsPart.length() > 0) {
            nsPart = urldecode(nsPart);
            idx = nsPart.indexOf(".");
            if (idx < 0) {
                database = nsPart;
                collection = null;
            } else {
                database = nsPart.substring(0, idx);
                collection = nsPart.substring(idx + 1);
            }
            MongoNamespace.checkDatabaseNameValidity(database);
        } else {
            database = null;
            collection = null;
        }

        String txtRecordsQueryParameters = isSrvProtocol
                ? new DefaultDnsResolver(dnsClient).resolveAdditionalQueryParametersFromTxtRecords(unresolvedHosts.get(0)) : "";
        String connectionStringQueryParameters = unprocessedConnectionString;

        Map<String, List<String>> connectionStringOptionsMap = parseOptions(connectionStringQueryParameters);
        Map<String, List<String>> txtRecordsOptionsMap = parseOptions(txtRecordsQueryParameters);
        if (!ALLOWED_OPTIONS_IN_TXT_RECORD.containsAll(txtRecordsOptionsMap.keySet())) {
            throw new MongoConfigurationException(format("A TXT record is only permitted to contain the keys %s, but the TXT record for "
            + "'%s' contains the keys %s", ALLOWED_OPTIONS_IN_TXT_RECORD, unresolvedHosts.get(0), txtRecordsOptionsMap.keySet()));
        }
        Map<String, List<String>> combinedOptionsMaps = combineOptionsMaps(txtRecordsOptionsMap, connectionStringOptionsMap);
        if (isSrvProtocol && !(combinedOptionsMaps.containsKey("tls") || combinedOptionsMaps.containsKey("ssl"))) {
            combinedOptionsMaps.put("tls", singletonList("true"));
        }
        translateOptions(combinedOptionsMaps);

        if (!isSrvProtocol && srvMaxHosts != null) {
            throw new IllegalArgumentException("srvMaxHosts can only be specified with mongodb+srv protocol");
        }

        if (!isSrvProtocol && srvServiceName != null) {
            throw new IllegalArgumentException("srvServiceName can only be specified with mongodb+srv protocol");
        }

        if (directConnection != null && directConnection) {
            if (isSrvProtocol) {
                throw new IllegalArgumentException("Direct connections are not supported when using mongodb+srv protocol");
            } else if (hosts.size() > 1) {
                throw new IllegalArgumentException("Direct connections are not supported when using multiple hosts");
            }
        }

        if (loadBalanced != null && loadBalanced) {
            if (directConnection != null && directConnection) {
                throw new IllegalArgumentException("directConnection=true can not be specified with loadBalanced=true");
            }
            if (requiredReplicaSetName != null) {
                throw new IllegalArgumentException("replicaSet can not be specified with loadBalanced=true");
            }
            if (hosts.size() > 1) {
                throw new IllegalArgumentException("Only one host can be specified with loadBalanced=true");
            }
            if (srvMaxHosts != null && srvMaxHosts > 0) {
                throw new IllegalArgumentException("srvMaxHosts can not be specified with loadBalanced=true");
            }
        }

        if (requiredReplicaSetName != null && srvMaxHosts != null && srvMaxHosts > 0) {
            throw new IllegalArgumentException("srvMaxHosts can not be specified with replica set name");
        }

        validateProxyParameters();

        credential = createCredentials(combinedOptionsMaps, userName, password);
        warnOnUnsupportedOptions(combinedOptionsMaps);
    }

    private static final Set<String> GENERAL_OPTIONS_KEYS = new LinkedHashSet<>();
    private static final Set<String> AUTH_KEYS = new HashSet<>();
    private static final Set<String> READ_PREFERENCE_KEYS = new HashSet<>();
    private static final Set<String> WRITE_CONCERN_KEYS = new HashSet<>();
    private static final Set<String> COMPRESSOR_KEYS = new HashSet<>();
    private static final Set<String> ALL_KEYS = new HashSet<>();

    static {
        GENERAL_OPTIONS_KEYS.add("minpoolsize");
        GENERAL_OPTIONS_KEYS.add("maxpoolsize");
        GENERAL_OPTIONS_KEYS.add("timeoutms");
        GENERAL_OPTIONS_KEYS.add("sockettimeoutms");
        GENERAL_OPTIONS_KEYS.add("waitqueuetimeoutms");
        GENERAL_OPTIONS_KEYS.add("connecttimeoutms");
        GENERAL_OPTIONS_KEYS.add("maxidletimems");
        GENERAL_OPTIONS_KEYS.add("maxlifetimems");
        GENERAL_OPTIONS_KEYS.add("maxconnecting");

        // Order matters here: Having tls after ssl means than the tls option will supersede the ssl option when both are set
        GENERAL_OPTIONS_KEYS.add("ssl");
        GENERAL_OPTIONS_KEYS.add("tls");

        // Order matters here: Having tlsinsecure before sslinvalidhostnameallowed and tlsallowinvalidhostnames means that those options
        // will supersede this one when both are set.
        GENERAL_OPTIONS_KEYS.add("tlsinsecure");

        // Order matters here: Having tlsallowinvalidhostnames after sslinvalidhostnameallowed means than the tlsallowinvalidhostnames
        // option will supersede the sslinvalidhostnameallowed option when both are set
        GENERAL_OPTIONS_KEYS.add("sslinvalidhostnameallowed");
        GENERAL_OPTIONS_KEYS.add("tlsallowinvalidhostnames");

        //Socks5 proxy settings
        GENERAL_OPTIONS_KEYS.add("proxyhost");
        GENERAL_OPTIONS_KEYS.add("proxyport");
        GENERAL_OPTIONS_KEYS.add("proxyusername");
        GENERAL_OPTIONS_KEYS.add("proxypassword");

        GENERAL_OPTIONS_KEYS.add("replicaset");
        GENERAL_OPTIONS_KEYS.add("readconcernlevel");

        GENERAL_OPTIONS_KEYS.add("serverselectiontimeoutms");
        GENERAL_OPTIONS_KEYS.add("localthresholdms");
        GENERAL_OPTIONS_KEYS.add("heartbeatfrequencyms");
        GENERAL_OPTIONS_KEYS.add("servermonitoringmode");
        GENERAL_OPTIONS_KEYS.add("retrywrites");
        GENERAL_OPTIONS_KEYS.add("retryreads");

        GENERAL_OPTIONS_KEYS.add("appname");

        GENERAL_OPTIONS_KEYS.add("uuidrepresentation");

        GENERAL_OPTIONS_KEYS.add("directconnection");
        GENERAL_OPTIONS_KEYS.add("loadbalanced");

        GENERAL_OPTIONS_KEYS.add("srvmaxhosts");
        GENERAL_OPTIONS_KEYS.add("srvservicename");

        COMPRESSOR_KEYS.add("compressors");
        COMPRESSOR_KEYS.add("zlibcompressionlevel");

        READ_PREFERENCE_KEYS.add("readpreference");
        READ_PREFERENCE_KEYS.add("readpreferencetags");
        READ_PREFERENCE_KEYS.add("maxstalenessseconds");

        WRITE_CONCERN_KEYS.add("safe");
        WRITE_CONCERN_KEYS.add("w");
        WRITE_CONCERN_KEYS.add("wtimeoutms");
        WRITE_CONCERN_KEYS.add("journal");

        AUTH_KEYS.add("authmechanism");
        AUTH_KEYS.add("authsource");
        AUTH_KEYS.add("gssapiservicename");
        AUTH_KEYS.add("authmechanismproperties");

        ALL_KEYS.addAll(GENERAL_OPTIONS_KEYS);
        ALL_KEYS.addAll(AUTH_KEYS);
        ALL_KEYS.addAll(READ_PREFERENCE_KEYS);
        ALL_KEYS.addAll(WRITE_CONCERN_KEYS);
        ALL_KEYS.addAll(COMPRESSOR_KEYS);
    }

    // Any options contained in the connection string completely replace the corresponding options specified in TXT records,
    // even for options which multiple values, e.g. readPreferenceTags
    private Map<String, List<String>> combineOptionsMaps(final Map<String, List<String>> txtRecordsOptionsMap,
                                                         final Map<String, List<String>> connectionStringOptionsMap) {
        Map<String, List<String>> combinedOptionsMaps = new HashMap<>(txtRecordsOptionsMap);
        combinedOptionsMaps.putAll(connectionStringOptionsMap);
        return combinedOptionsMaps;
    }


    private void warnOnUnsupportedOptions(final Map<String, List<String>> optionsMap) {
        if (LOGGER.isWarnEnabled()) {
            optionsMap.keySet()
                    .stream()
                    .filter(k -> !ALL_KEYS.contains(k))
                    .forEach(k -> LOGGER.warn(format("Connection string contains unsupported option '%s'.", k)));
        }
    }

    private void translateOptions(final Map<String, List<String>> optionsMap) {
        boolean tlsInsecureSet = false;
        boolean tlsAllowInvalidHostnamesSet = false;

        for (final String key : GENERAL_OPTIONS_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }
            switch (key) {
                case "maxpoolsize":
                    maxConnectionPoolSize = parseInteger(value, "maxpoolsize");
                    break;
                case "minpoolsize":
                    minConnectionPoolSize = parseInteger(value, "minpoolsize");
                    break;
                case "maxidletimems":
                    maxConnectionIdleTime = parseInteger(value, "maxidletimems");
                    break;
                case "maxlifetimems":
                    maxConnectionLifeTime = parseInteger(value, "maxlifetimems");
                    break;
                case "maxconnecting":
                    maxConnecting = parseInteger(value, "maxConnecting");
                    break;
                case "waitqueuetimeoutms":
                    maxWaitTime = parseInteger(value, "waitqueuetimeoutms");
                    break;
                case "connecttimeoutms":
                    connectTimeout = parseInteger(value, "connecttimeoutms");
                    break;
                case "sockettimeoutms":
                    socketTimeout = parseInteger(value, "sockettimeoutms");
                    break;
                case "timeoutms":
                    timeout = parseLong(value, "timeoutms");
                    break;
                case "proxyhost":
                    proxyHost = value;
                    break;
                case "proxyport":
                    proxyPort = parseInteger(value, "proxyPort");
                    break;
                case "proxyusername":
                    proxyUsername = value;
                    break;
                case "proxypassword":
                    proxyPassword = value;
                    break;
                case "tlsallowinvalidhostnames":
                    sslInvalidHostnameAllowed = parseBoolean(value, "tlsAllowInvalidHostnames");
                    tlsAllowInvalidHostnamesSet = true;
                    break;
                case "sslinvalidhostnameallowed":
                    sslInvalidHostnameAllowed = parseBoolean(value, "sslinvalidhostnameallowed");
                    tlsAllowInvalidHostnamesSet = true;
                    break;
                case "tlsinsecure":
                    sslInvalidHostnameAllowed = parseBoolean(value, "tlsinsecure");
                    tlsInsecureSet = true;
                    break;
                case "ssl":
                    initializeSslEnabled("ssl", value);
                    break;
                case "tls":
                    initializeSslEnabled("tls", value);
                    break;
                case "replicaset":
                    requiredReplicaSetName = value;
                    break;
                case "readconcernlevel":
                    readConcern = new ReadConcern(ReadConcernLevel.fromString(value));
                    break;
                case "serverselectiontimeoutms":
                    serverSelectionTimeout = parseInteger(value, "serverselectiontimeoutms");
                    break;
                case "localthresholdms":
                    localThreshold = parseInteger(value, "localthresholdms");
                    break;
                case "heartbeatfrequencyms":
                    heartbeatFrequency = parseInteger(value, "heartbeatfrequencyms");
                    break;
                case "servermonitoringmode":
                    serverMonitoringMode = ServerMonitoringModeUtil.fromString(value);
                    break;
                case "appname":
                    applicationName = value;
                    break;
                case "retrywrites":
                    retryWrites = parseBoolean(value, "retrywrites");
                    break;
                case "retryreads":
                    retryReads = parseBoolean(value, "retryreads");
                    break;
                case "uuidrepresentation":
                    uuidRepresentation = createUuidRepresentation(value);
                    break;
                case "directconnection":
                    directConnection = parseBoolean(value, "directconnection");
                    break;
                case "loadbalanced":
                    loadBalanced = parseBoolean(value, "loadbalanced");
                    break;
                case "srvmaxhosts":
                    srvMaxHosts = parseInteger(value, "srvmaxhosts");
                    if (srvMaxHosts < 0) {
                        throw new IllegalArgumentException("srvMaxHosts must be >= 0");
                    }
                    break;
                case "srvservicename":
                    srvServiceName = value;
                    break;
                default:
                    break;
            }
        }

        if (tlsInsecureSet && tlsAllowInvalidHostnamesSet) {
            throw new IllegalArgumentException("tlsAllowInvalidHostnames or sslInvalidHostnameAllowed set along with tlsInsecure "
                    + "is not allowed");
        }

        writeConcern = createWriteConcern(optionsMap);
        readPreference = createReadPreference(optionsMap);
        compressorList = createCompressors(optionsMap);
    }

    private void initializeSslEnabled(final String key, final String value) {
        Boolean booleanValue = parseBoolean(value, key);
        if (sslEnabled != null && !sslEnabled.equals(booleanValue)) {
            throw new IllegalArgumentException("Conflicting tls and ssl parameter values are not allowed");
        }
        sslEnabled = booleanValue;
    }

    private List<MongoCompressor> createCompressors(final Map<String, List<String>> optionsMap) {
        String compressors = "";
        Integer zlibCompressionLevel = null;

        for (final String key : COMPRESSOR_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("compressors")) {
                compressors = value;
            } else if (key.equals("zlibcompressionlevel")) {
                zlibCompressionLevel = Integer.parseInt(value);
            }
        }
        return buildCompressors(compressors, zlibCompressionLevel);
    }

    private List<MongoCompressor> buildCompressors(final String compressors, @Nullable final Integer zlibCompressionLevel) {
        List<MongoCompressor> compressorsList = new ArrayList<>();

        for (String cur : compressors.split(",")) {
            if (cur.equals("zlib")) {
                MongoCompressor zlibCompressor = MongoCompressor.createZlibCompressor();
                if (zlibCompressionLevel != null) {
                    zlibCompressor = zlibCompressor.withProperty(MongoCompressor.LEVEL, zlibCompressionLevel);
                }
                compressorsList.add(zlibCompressor);
            } else if (cur.equals("snappy")) {
                compressorsList.add(MongoCompressor.createSnappyCompressor());
            } else if (cur.equals("zstd")) {
                compressorsList.add(MongoCompressor.createZstdCompressor());
            } else if (!cur.isEmpty()) {
                throw new IllegalArgumentException("Unsupported compressor '" + cur + "'");
            }
        }

        return unmodifiableList(compressorsList);
    }

    @Nullable
    private WriteConcern createWriteConcern(final Map<String, List<String>> optionsMap) {
        String w = null;
        Integer wTimeout = null;
        Boolean safe = null;
        Boolean journal = null;

        for (final String key : WRITE_CONCERN_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            switch (key) {
                case "safe":
                    safe = parseBoolean(value, "safe");
                    break;
                case "w":
                    w = value;
                    break;
                case "wtimeoutms":
                    wTimeout = Integer.parseInt(value);
                    break;
                case "journal":
                    journal = parseBoolean(value, "journal");
                    break;
                default:
                    break;
            }
        }
        return buildWriteConcern(safe, w, wTimeout, journal);
    }

    @Nullable
    private ReadPreference createReadPreference(final Map<String, List<String>> optionsMap) {
        String readPreferenceType = null;
        List<TagSet> tagSetList = new ArrayList<>();
        long maxStalenessSeconds = -1;

        for (final String key : READ_PREFERENCE_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            switch (key) {
                case "readpreference":
                    readPreferenceType = value;
                    break;
                case "maxstalenessseconds":
                    maxStalenessSeconds = parseInteger(value, "maxstalenessseconds");
                    break;
                case "readpreferencetags":
                    for (final String cur : optionsMap.get(key)) {
                        TagSet tagSet = getTags(cur.trim());
                        tagSetList.add(tagSet);
                    }
                    break;
                default:
                    break;
            }
        }
        return buildReadPreference(readPreferenceType, tagSetList, maxStalenessSeconds);
    }

    private UuidRepresentation createUuidRepresentation(final String value) {
        if (value.equalsIgnoreCase("unspecified")) {
            return UuidRepresentation.UNSPECIFIED;
        }
        if (value.equalsIgnoreCase("javaLegacy")) {
            return UuidRepresentation.JAVA_LEGACY;
        }
        if (value.equalsIgnoreCase("csharpLegacy")) {
            return UuidRepresentation.C_SHARP_LEGACY;
        }
        if (value.equalsIgnoreCase("pythonLegacy")) {
            return UuidRepresentation.PYTHON_LEGACY;
        }
        if (value.equalsIgnoreCase("standard")) {
            return UuidRepresentation.STANDARD;
        }
        throw new IllegalArgumentException("Unknown uuid representation: " + value);
    }

    @Nullable
    private MongoCredential createCredentials(final Map<String, List<String>> optionsMap, @Nullable final String userName,
                                              @Nullable final char[] password) {
        AuthenticationMechanism mechanism = null;
        String authSource = null;
        String gssapiServiceName = null;
        String authMechanismProperties = null;

        for (final String key : AUTH_KEYS) {
            String value = getLastValue(optionsMap, key);

            if (value == null) {
                continue;
            }

            switch (key) {
                case "authmechanism":
                    if (value.equals("MONGODB-CR")) {
                        if (userName == null) {
                            throw new IllegalArgumentException("username can not be null");
                        }
                        LOGGER.warn("Deprecated MONGDOB-CR authentication mechanism used in connection string");
                    } else {
                        mechanism = AuthenticationMechanism.fromMechanismName(value);
                    }
                    break;
                case "authsource":
                    if (value.equals("")) {
                        throw new IllegalArgumentException("authSource can not be an empty string");
                    }
                    authSource = value;
                    break;
                case "gssapiservicename":
                    gssapiServiceName = value;
                    break;
                case "authmechanismproperties":
                    authMechanismProperties = value;
                    break;
                default:
                    break;
            }
        }

        MongoCredential credential = null;
        if (mechanism != null) {
            credential = createMongoCredentialWithMechanism(mechanism, userName, password, authSource, gssapiServiceName);
        } else if (userName != null) {
            credential = MongoCredential.createCredential(userName,
                    getAuthSourceOrDefault(authSource, database != null ? database : "admin"), password);
        }

        if (credential != null && authMechanismProperties != null) {
            for (String part : authMechanismProperties.split(",")) {
                String[] mechanismPropertyKeyValue = part.split(":", 2);
                if (mechanismPropertyKeyValue.length != 2) {
                    throw new IllegalArgumentException(format("The connection string contains invalid authentication properties. "
                            + "'%s' is not a key value pair", part));
                }
                String key = mechanismPropertyKeyValue[0].trim().toLowerCase();
                String value = mechanismPropertyKeyValue[1].trim();
                if (MECHANISM_KEYS_DISALLOWED_IN_CONNECTION_STRING.contains(key)) {
                    throw new IllegalArgumentException(format("The connection string contains disallowed mechanism properties. "
                            + "'%s' must be set on the credential programmatically.", key));
                }

                if (key.equals("canonicalize_host_name")) {
                    credential = credential.withMechanismProperty(key, Boolean.valueOf(value));
                } else {
                    credential = credential.withMechanismProperty(key, value);
                }
            }
        }
        return credential;
    }

    private MongoCredential createMongoCredentialWithMechanism(final AuthenticationMechanism mechanism, final String userName,
                                                               @Nullable final char[] password,
                                                               @Nullable final String authSource,
                                                               @Nullable final String gssapiServiceName) {
        MongoCredential credential;
        String mechanismAuthSource;
        switch (mechanism) {
            case PLAIN:
                mechanismAuthSource = getAuthSourceOrDefault(authSource, database != null ? database : "$external");
                break;
            case GSSAPI:
            case MONGODB_X509:
                mechanismAuthSource = getAuthSourceOrDefault(authSource, "$external");
                if (!mechanismAuthSource.equals("$external")) {
                    throw new IllegalArgumentException(format("Invalid authSource for %s, it must be '$external'", mechanism));
                }
                break;
            default:
                mechanismAuthSource = getAuthSourceOrDefault(authSource, database != null ? database : "admin");
        }

        switch (mechanism) {
            case GSSAPI:
                credential = MongoCredential.createGSSAPICredential(userName);
                if (gssapiServiceName != null) {
                    credential = credential.withMechanismProperty("SERVICE_NAME", gssapiServiceName);
                }
                if (password != null && LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Password in connection string not used with MONGODB_X509 authentication mechanism.");
                }
                break;
            case PLAIN:
                credential = MongoCredential.createPlainCredential(userName, mechanismAuthSource, password);
                break;
            case MONGODB_X509:
                if (password != null) {
                    throw new IllegalArgumentException("Invalid mechanism, MONGODB_x509 does not support passwords");
                }
                credential = MongoCredential.createMongoX509Credential(userName);
                break;
            case SCRAM_SHA_1:
                credential = MongoCredential.createScramSha1Credential(userName, mechanismAuthSource, password);
                break;
            case SCRAM_SHA_256:
                credential = MongoCredential.createScramSha256Credential(userName, mechanismAuthSource, password);
                break;
            case MONGODB_AWS:
                credential = MongoCredential.createAwsCredential(userName, password);
                break;
            case MONGODB_OIDC:
                validateCreateOidcCredential(password);
                credential = MongoCredential.createOidcCredential(userName);
                break;
            default:
                throw new UnsupportedOperationException(format("The connection string contains an invalid authentication mechanism'. "
                                                                       + "'%s' is not a supported authentication mechanism",
                        mechanism));
        }
        return credential;
    }

    private String getAuthSourceOrDefault(@Nullable final String authSource, final String defaultAuthSource) {
        if (authSource != null) {
            return authSource;
        } else {
            return defaultAuthSource;
        }
    }

    @Nullable
    private String getLastValue(final Map<String, List<String>> optionsMap, final String key) {
        List<String> valueList = optionsMap.get(key);
        if (valueList == null) {
            return null;
        }
        return valueList.get(valueList.size() - 1);
    }

    private Map<String, List<String>> parseOptions(final String optionsPart) {
        Map<String, List<String>> optionsMap = new HashMap<>();
        if (optionsPart.isEmpty()) {
            return optionsMap;
        }

        for (final String part : optionsPart.split("&|;")) {
            if (part.isEmpty()) {
                continue;
            }
            int idx = part.indexOf("=");
            if (idx >= 0) {
                String key = part.substring(0, idx).toLowerCase();
                String value = part.substring(idx + 1);
                List<String> valueList = optionsMap.get(key);
                if (valueList == null) {
                    valueList = new ArrayList<>(1);
                }
                valueList.add(urldecode(value));
                optionsMap.put(key, valueList);
            } else {
                throw new IllegalArgumentException(format("The connection string contains an invalid option '%s'. "
                        + "'%s' is missing the value delimiter eg '%s=value'", optionsPart, part, part));
            }
        }

        // handle legacy wtimeout settings
        if (optionsMap.containsKey("wtimeout") && !optionsMap.containsKey("wtimeoutms")) {
            optionsMap.put("wtimeoutms", optionsMap.remove("wtimeout"));
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Uri option 'wtimeout' has been deprecated, use 'wtimeoutms' instead.");
            }
        }
        // handle legacy j settings
        if (optionsMap.containsKey("j") && !optionsMap.containsKey("journal")) {
            optionsMap.put("journal", optionsMap.remove("j"));
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Uri option 'j' has been deprecated, use 'journal' instead.");
            }
        }

        return optionsMap;
    }

    @Nullable
    private ReadPreference buildReadPreference(@Nullable final String readPreferenceType,
                                               final List<TagSet> tagSetList, final long maxStalenessSeconds) {
        if (readPreferenceType != null) {
            if (tagSetList.isEmpty() && maxStalenessSeconds == -1) {
                return ReadPreference.valueOf(readPreferenceType);
            } else if (maxStalenessSeconds == -1) {
               return ReadPreference.valueOf(readPreferenceType, tagSetList);
            } else {
                return ReadPreference.valueOf(readPreferenceType, tagSetList, maxStalenessSeconds, TimeUnit.SECONDS);
            }
        } else if (!(tagSetList.isEmpty() && maxStalenessSeconds == -1)) {
            throw new IllegalArgumentException("Read preference mode must be specified if "
                                                       + "either read preference tags or max staleness is specified");
        }
        return null;
    }

    @Nullable
    private WriteConcern buildWriteConcern(@Nullable final Boolean safe, @Nullable final String w,
                                           @Nullable final Integer wTimeout,
                                           @Nullable final Boolean journal) {
        WriteConcern retVal = null;
        if (w != null || wTimeout != null || journal != null) {
            if (w == null) {
                retVal = WriteConcern.ACKNOWLEDGED;
            } else {
                try {
                    retVal = new WriteConcern(Integer.parseInt(w));
                } catch (NumberFormatException e) {
                    retVal = new WriteConcern(w);
                }
            }
            if (wTimeout != null) {
                retVal = retVal.withWTimeout(wTimeout, TimeUnit.MILLISECONDS);
            }
            if (journal != null) {
                retVal = retVal.withJournal(journal);
            }
            return retVal;
        } else if (safe != null) {
            if (safe) {
                retVal = WriteConcern.ACKNOWLEDGED;
            } else {
                retVal = WriteConcern.UNACKNOWLEDGED;
            }
        }
        return retVal;
    }

    private TagSet getTags(final String tagSetString) {
        List<Tag> tagList = new ArrayList<>();
        if (tagSetString.length() > 0) {
            for (final String tag : tagSetString.split(",")) {
                String[] tagKeyValuePair = tag.split(":");
                if (tagKeyValuePair.length != 2) {
                    throw new IllegalArgumentException(format("The connection string contains an invalid read preference tag. "
                            + "'%s' is not a key value pair", tagSetString));
                }
                tagList.add(new Tag(tagKeyValuePair[0].trim(), tagKeyValuePair[1].trim()));
            }
        }
        return new TagSet(tagList);
    }

    private static final Set<String> TRUE_VALUES = new HashSet<>(asList("true", "yes", "1"));
    private static final Set<String> FALSE_VALUES = new HashSet<>(asList("false", "no", "0"));

    @Nullable
    private Boolean parseBoolean(final String input, final String key) {
        String trimmedInput = input.trim().toLowerCase();

        if (TRUE_VALUES.contains(trimmedInput)) {
            if (!trimmedInput.equals("true")) {
                LOGGER.warn(format("Deprecated boolean value '%s' in the connection string for '%s'. Replace with 'true'",
                        trimmedInput, key));
            }
            return true;
        } else if (FALSE_VALUES.contains(trimmedInput)) {
            if (!trimmedInput.equals("false")) {
                LOGGER.warn(format("Deprecated boolean value '%s' in the connection string for '%s'. Replace with'false'",
                        trimmedInput, key));
            }
            return false;
        } else {
            LOGGER.warn(format("Ignoring unrecognized boolean value '%s' in the connection string for '%s'. "
                    + "Replace with either 'true' or 'false'", trimmedInput, key));
            return null;
        }
    }

    private int parseInteger(final String input, final String key) {
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("The connection string contains an invalid value for '%s'. "
                    + "'%s' is not a valid integer", key, input));
        }
    }

    private long parseLong(final String input, final String key) {
        try {
            return Long.parseLong(input);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("The connection string contains an invalid value for '%s'. "
                    + "'%s' is not a valid long", key, input));
        }
    }

    private List<String> parseHosts(final List<String> rawHosts) {
        if (rawHosts.size() == 0){
            throw new IllegalArgumentException("The connection string must contain at least one host");
        }
        List<String> hosts = new ArrayList<>();
        for (String host : rawHosts) {
            if (host.length() == 0) {
                throw new IllegalArgumentException(format("The connection string contains an empty host '%s'. ", rawHosts));
            } else if (host.endsWith(".sock")) {
                host = urldecode(host);
            } else if (host.startsWith("[")) {
                if (!host.contains("]")) {
                    throw new IllegalArgumentException(format("The connection string contains an invalid host '%s'. "
                            + "IPv6 address literals must be enclosed in '[' and ']' according to RFC 2732", host));
                }
                int idx = host.indexOf("]:");
                if (idx != -1) {
                    validatePort(host.substring(idx + 2));
                }
            } else {
                int colonCount = countOccurrences(host, ":");
                if (colonCount > 1) {
                    throw new IllegalArgumentException(format("The connection string contains an invalid host '%s'. "
                            + "Reserved characters such as ':' must be escaped according RFC 2396. "
                            + "Any IPv6 address literal must be enclosed in '[' and ']' according to RFC 2732.", host));
                } else if (colonCount == 1) {
                    validatePort(host.substring(host.indexOf(":") + 1));
                }
            }
            hosts.add(host);
        }
        Collections.sort(hosts);
        return hosts;
    }

    private void validatePort(final String port) {
        try {
            int portInt = Integer.parseInt(port);
            if (portInt <= 0 || portInt > 65535) {
                throw new IllegalArgumentException("The connection string contains an invalid host and port. "
                        + "The port must be an integer between 0 and 65535.");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The connection string contains an invalid host and port. "
                    + "The port contains non-digit characters, it must be an integer between 0 and 65535. "
                    + "Hint: username and password must be escaped according to RFC 3986.");
        }
    }

    private void validateProxyParameters() {
        if (proxyHost == null) {
            if (proxyPort != null) {
                throw new IllegalArgumentException("proxyPort can only be specified with proxyHost");
            } else if (proxyUsername != null) {
                throw new IllegalArgumentException("proxyUsername can only be specified with proxyHost");
            } else if (proxyPassword != null) {
                throw new IllegalArgumentException("proxyPassword can only be specified with proxyHost");
            }
        }
        if (proxyPort != null && (proxyPort < 0 || proxyPort > 65535)) {
            throw new IllegalArgumentException("proxyPort should be within the valid range (0 to 65535)");
        }
        if (proxyUsername != null) {
            if (proxyUsername.isEmpty()) {
                throw new IllegalArgumentException("proxyUsername cannot be empty");
            }
            if (proxyUsername.getBytes(StandardCharsets.UTF_8).length >= 255) {
                throw new IllegalArgumentException("username's length in bytes cannot be greater than 255");
            }
        }
        if (proxyPassword != null) {
            if (proxyPassword.isEmpty()) {
                throw new IllegalArgumentException("proxyPassword cannot be empty");
            }
            if (proxyPassword.getBytes(StandardCharsets.UTF_8).length >= 255) {
                throw new IllegalArgumentException("password's length in bytes cannot be greater than 255");
            }
        }
        if (proxyUsername == null ^ proxyPassword == null) {
            throw new IllegalArgumentException(
                    "Both proxyUsername and proxyPassword must be set together. They cannot be set individually");
        }
    }

    private int countOccurrences(final String haystack, final String needle) {
        return haystack.length() - haystack.replace(needle, "").length();
    }

    private String urldecode(final String input) {
        return urldecode(input, false);
    }

    private String urldecode(final String input, final boolean password) {
        try {
            return URLDecoder.decode(input, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            if (password) {
                throw new IllegalArgumentException("The connection string contained unsupported characters in the password.");
            } else {
                throw new IllegalArgumentException(format("The connection string contained unsupported characters: '%s'."
                        + "Decoding produced the following error: %s", input, e.getMessage()));
            }
        }
    }

    // ---------------------------------

    /**
     * Gets the username
     *
     * @return the username
     */
    @Nullable
    public String getUsername() {
        return credential != null ? credential.getUserName() : null;
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    @Nullable
    public char[] getPassword() {
        return credential != null ? credential.getPassword() : null;
    }

    /**
     * Returns true if the connection string requires SRV protocol to resolve the host lists from the configured host.
     *
     * @return true if SRV protocol is required to resolve hosts.
     */
    public boolean isSrvProtocol() {
        return isSrvProtocol;
    }

    /**
     * Gets the maximum number of hosts to connect to when using SRV protocol.
     *
     * @return the maximum number of hosts to connect to when using SRV protocol.  Defaults to null.
     * @since 4.4
     */
    @Nullable
    public Integer getSrvMaxHosts() {
        return srvMaxHosts;
    }

    /**
     * Gets the SRV service name.
     *
     * @return the SRV service name.  Defaults to null in the connection string, but defaults to {@code "mongodb"} in
     * {@link ClusterSettings}.
     * @since 4.5
     * @see ClusterSettings#getSrvServiceName()
     */
    @Nullable
    public String getSrvServiceName() {
        return srvServiceName;
    }

    /**
     * Gets the list of hosts
     *
     * @return the host list
     */
    public List<String> getHosts() {
        return hosts;
    }

    /**
     * Gets the database name
     *
     * @return the database name
     */
    @Nullable
    public String getDatabase() {
        return database;
    }

    /**
     * Gets the collection name
     *
     * @return the collection name
     */
    @Nullable
    public String getCollection() {
        return collection;
    }

    /**
     * Indicates if the connection should be a direct connection
     *
     * @return true if a direct connection
     * @since 4.1
     */
    @Nullable
    public Boolean isDirectConnection() {
        return directConnection;
    }

    /**
     * Indicates if the connection is through a load balancer.
     *
     * @return true if a load-balanced connection
     * @since 4.3
     */
    @Nullable
    public Boolean isLoadBalanced() {
        return loadBalanced;
    }

    /**
     * Get the unparsed connection string.
     *
     * @return the connection string
     * @since 3.1
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * Gets the credential or null if no credentials were specified in the connection string.
     *
     * @return the credentials in an immutable list
     * @since 3.6
     */
    @Nullable
    public MongoCredential getCredential() {
        return credential;
    }

    /**
     * Gets the read preference specified in the connection string.
     * @return the read preference
     */
    @Nullable
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets the read concern specified in the connection string.
     * @return the read concern
     */
    @Nullable
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Gets the write concern specified in the connection string.
     * @return the write concern
     */
    @Nullable
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * <p>Gets whether writes should be retried if they fail due to a network error</p>
     *
     * The name of this method differs from others in this class so as not to conflict with the now removed
     * getRetryWrites() method, which returned a primitive {@code boolean} value, and didn't allow callers to differentiate
     * between a false value and an unset value.
     *
     * @return the retryWrites value, or null if unset
     * @since 3.9
     * @mongodb.server.release 3.6
     */
    @Nullable
    public Boolean getRetryWritesValue() {
        return retryWrites;
    }

    /**
     * <p>Gets whether reads should be retried if they fail due to a network error</p>
     *
     * @return the retryWrites value
     * @since 3.11
     * @mongodb.server.release 3.6
     */
    @Nullable
    public Boolean getRetryReads() {
        return retryReads;
    }

    /**
     * Gets the minimum connection pool size specified in the connection string.
     * @return the minimum connection pool size
     */
    @Nullable
    public Integer getMinConnectionPoolSize() {
        return minConnectionPoolSize;
    }

    /**
     * Gets the maximum connection pool size specified in the connection string.
     * @return the maximum connection pool size
     * @see ConnectionPoolSettings#getMaxSize()
     */
    @Nullable
    public Integer getMaxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    /**
     * The maximum duration to wait until either:
     * <ul>
     *     <li>
     *         an {@linkplain ConnectionCheckedOutEvent in-use connection} becomes {@linkplain ConnectionCheckedInEvent available}; or
     *     </li>
     *     <li>
     *         a {@linkplain ConnectionCreatedEvent connection is created} and begins to be {@linkplain ConnectionReadyEvent established}.
     *         The time between {@linkplain ConnectionCheckOutStartedEvent requesting} a connection
     *         and it being created is limited by this maximum duration.
     *         The maximum time between it being created and {@linkplain ConnectionCheckedOutEvent successfully checked out},
     *         which includes the time to {@linkplain ConnectionReadyEvent establish} the created connection,
     *         is affected by {@link SocketSettings#getConnectTimeout(TimeUnit)}, {@link SocketSettings#getReadTimeout(TimeUnit)}
     *         among others, and is not affected by this maximum duration.
     *     </li>
     * </ul>
     * The reasons it is not always possible to create and start establishing a connection
     * whenever there is no available connection:
     * <ul>
     *     <li>
     *         the number of connections per pool is limited by {@link #getMaxConnectionPoolSize()};
     *     </li>
     *     <li>
     *         the number of connections a pool may be establishing concurrently is limited by {@link #getMaxConnecting()}.
     *     </li>
     * </ul>
     *
     * @return The value of the {@code waitQueueTimeoutMS} option, if specified.
     * @see ConnectionPoolSettings#getMaxWaitTime(TimeUnit)
     */
    @Nullable
    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     * Gets the maximum connection idle time specified in the connection string.
     * @return the maximum connection idle time
     */
    @Nullable
    public Integer getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    /**
     * Gets the maximum connection lifetime specified in the connection string.
     *
     * @return the maximum connection lifetime
     */
    @Nullable
    public Integer getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }

    /**
     * Gets the maximum number of connections a pool may be establishing concurrently specified in the connection string.
     * @return The maximum number of connections a pool may be establishing concurrently
     * if the {@code maxConnecting} option is specified in the connection string, or {@code null} otherwise.
     * @see ConnectionPoolSettings#getMaxConnecting()
     * @since 4.4
     */
    @Nullable
    public Integer getMaxConnecting() {
        return maxConnecting;
    }

    /**
     * The time limit for the full execution of an operation in milliseconds.
     *
     * <p>If set the following deprecated options will be ignored:
     * {@code waitQueueTimeoutMS}, {@code socketTimeoutMS}, {@code wTimeoutMS}, {@code maxTimeMS} and {@code maxCommitTimeMS}</p>
     *
     * <ul>
     *   <li>{@code null} means that the timeout mechanism for operations will defer to using:
     *    <ul>
     *        <li>{@code waitQueueTimeoutMS}: The maximum wait time in milliseconds that a thread may wait for a connection to become
     *        available</li>
     *        <li>{@code socketTimeoutMS}: How long a send or receive on a socket can take before timing out.</li>
     *        <li>{@code wTimeoutMS}: How long the server will wait for the write concern to be fulfilled before timing out.</li>
     *        <li>{@code maxTimeMS}: The cumulative time limit for processing operations on a cursor.
     *        See: <a href="https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS">cursor.maxTimeMS</a>.</li>
     *        <li>{@code maxCommitTimeMS}: The maximum amount of time to allow a single {@code commitTransaction} command to execute.
     *        See: {@link TransactionOptions#getMaxCommitTime}.</li>
     *   </ul>
     *   </li>
     *   <li>{@code 0} means infinite timeout.</li>
     *    <li>{@code > 0} The time limit to use for the full execution of an operation.</li>
     * </ul>
     *
     * @return the time limit for the full execution of an operation in milliseconds or null.
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    @Nullable
    public Long getTimeout() {
        return timeout;
    }

    /**
     * Gets the socket connect timeout specified in the connection string.
     * @return the socket connect timeout
     */
    @Nullable
    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Gets the socket timeout specified in the connection string.
     * @return the socket timeout
     */
    @Nullable
    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * Gets the SSL enabled value specified in the connection string.
     * @return the SSL enabled value
     */
    @Nullable
    public Boolean getSslEnabled() {
        return sslEnabled;
    }

    /**
     * Gets the SOCKS5 proxy host specified in the connection string.
     *
     * @return the proxy host value.
     * @since 4.11
     */
    @Nullable
    public String getProxyHost() {
        return proxyHost;
    }

    /**
     * Gets the SOCKS5 proxy port specified in the connection string.
     *
     * @return the proxy port value.
     * @since 4.11
     */
    @Nullable
    public Integer getProxyPort() {
        return proxyPort;
    }

    /**
     * Gets the SOCKS5 proxy username specified in the connection string.
     *
     * @return the proxy username value.
     * @since 4.11
     */
    @Nullable
    public String getProxyUsername() {
        return proxyUsername;
    }

    /**
     * Gets the SOCKS5 proxy password specified in the connection string.
     *
     * @return the proxy password value.
     * @since 4.11
     */
    @Nullable
    public String getProxyPassword() {
        return proxyPassword;
    }
    /**
     * Gets the SSL invalidHostnameAllowed value specified in the connection string.
     *
     * @return the SSL invalidHostnameAllowed value
     * @since 3.3
     */
    @Nullable
    public Boolean getSslInvalidHostnameAllowed() {
        return sslInvalidHostnameAllowed;
    }

    /**
     * Gets the required replica set name specified in the connection string.
     * @return the required replica set name
     */
    @Nullable
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    /**
     *
     * @return the server selection timeout (in milliseconds), or null if unset
     * @since 3.3
     */
    @Nullable
    public Integer getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }

    /**
     *
     * @return the local threshold (in milliseconds), or null if unset
     * since 3.3
     */
    @Nullable
    public Integer getLocalThreshold() {
        return localThreshold;
    }

    /**
     *
     * @return the heartbeat frequency (in milliseconds), or null if unset
     * since 3.3
     */
    @Nullable
    public Integer getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    /**
     * The server monitoring mode, which defines the monitoring protocol to use.
     * <p>
     * Default is {@link ServerMonitoringMode#AUTO}.</p>
     *
     * @return The {@link ServerMonitoringMode}, or {@code null} if unset and the default is to be used.
     * @see ServerSettings#getServerMonitoringMode()
     * @since 5.1
     */
    @Nullable
    public ServerMonitoringMode getServerMonitoringMode() {
        return serverMonitoringMode;
    }

    /**
     * Gets the logical name of the application.  The application name may be used by the client to identify the application to the server,
     * for use in server logs, slow query logs, and profile collection.
     *
     * <p>Default is null.</p>
     *
     * @return the application name, which may be null
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    @Nullable
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Gets the list of compressors.
     *
     * @return the non-null list of compressors
     * @since 3.6
     */
    public List<MongoCompressor> getCompressorList() {
        return compressorList;
    }

    /**
     * Gets the UUID representation.
     *
     * <p>Default is null.</p>
     *
     * @return the UUID representation, which may be null if it was unspecified
     * @since 3.12
     */
    @Nullable
    public UuidRepresentation getUuidRepresentation() {
        return uuidRepresentation;
    }

    @Override
    public String toString() {
        return connectionString;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectionString that = (ConnectionString) o;
        return isSrvProtocol == that.isSrvProtocol
                && Objects.equals(directConnection, that.directConnection)
                && Objects.equals(credential, that.credential)
                && Objects.equals(hosts, that.hosts)
                && Objects.equals(database, that.database)
                && Objects.equals(collection, that.collection)
                && Objects.equals(readPreference, that.readPreference)
                && Objects.equals(writeConcern, that.writeConcern)
                && Objects.equals(retryWrites, that.retryWrites)
                && Objects.equals(retryReads, that.retryReads)
                && Objects.equals(readConcern, that.readConcern)
                && Objects.equals(minConnectionPoolSize, that.minConnectionPoolSize)
                && Objects.equals(maxConnectionPoolSize, that.maxConnectionPoolSize)
                && Objects.equals(maxWaitTime, that.maxWaitTime)
                && Objects.equals(maxConnectionIdleTime, that.maxConnectionIdleTime)
                && Objects.equals(maxConnectionLifeTime, that.maxConnectionLifeTime)
                && Objects.equals(maxConnecting, that.maxConnecting)
                && Objects.equals(connectTimeout, that.connectTimeout)
                && Objects.equals(timeout, that.timeout)
                && Objects.equals(socketTimeout, that.socketTimeout)
                && Objects.equals(proxyHost, that.proxyHost)
                && Objects.equals(proxyPort, that.proxyPort)
                && Objects.equals(proxyUsername, that.proxyUsername)
                && Objects.equals(proxyPassword, that.proxyPassword)
                && Objects.equals(sslEnabled, that.sslEnabled)
                && Objects.equals(sslInvalidHostnameAllowed, that.sslInvalidHostnameAllowed)
                && Objects.equals(requiredReplicaSetName, that.requiredReplicaSetName)
                && Objects.equals(serverSelectionTimeout, that.serverSelectionTimeout)
                && Objects.equals(localThreshold, that.localThreshold)
                && Objects.equals(heartbeatFrequency, that.heartbeatFrequency)
                && Objects.equals(serverMonitoringMode, that.serverMonitoringMode)
                && Objects.equals(applicationName, that.applicationName)
                && Objects.equals(compressorList, that.compressorList)
                && Objects.equals(uuidRepresentation, that.uuidRepresentation)
                && Objects.equals(srvServiceName, that.srvServiceName)
                && Objects.equals(srvMaxHosts, that.srvMaxHosts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(credential, isSrvProtocol, hosts, database, collection, directConnection, readPreference,
                writeConcern, retryWrites, retryReads, readConcern, minConnectionPoolSize, maxConnectionPoolSize, maxWaitTime,
                maxConnectionIdleTime, maxConnectionLifeTime, maxConnecting, connectTimeout, timeout, socketTimeout, sslEnabled,
                sslInvalidHostnameAllowed, requiredReplicaSetName, serverSelectionTimeout, localThreshold, heartbeatFrequency,
                serverMonitoringMode, applicationName, compressorList, uuidRepresentation, srvServiceName, srvMaxHosts, proxyHost,
                proxyPort, proxyUsername, proxyPassword);
    }
}
