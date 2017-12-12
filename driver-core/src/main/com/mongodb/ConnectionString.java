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

import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.dns.DnsResolver.resolveAdditionalQueryParametersFromTxtRecords;
import static com.mongodb.internal.dns.DnsResolver.resolveHostFromSrvRecords;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;


/**
 * <p>Represents a <a href="http://www.mongodb.org/display/DOCS/Connections">Connection String</a>. The Connection String describes the
 * hosts to be used and options.</p>
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
 * <li>{@code host1} is the only required part of the connection string.  It identifies a server address to connect to.</li>
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
 * <p>Connection Configuration:</p>
 * <p>Connection Configuration:</p>
 * <ul>
 * <li>{@code streamType=nio2|netty}: The stream type to use for connections. If unspecified, nio2 will be used.</li>
 * <li>{@code ssl=true|false}: Whether to connect using SSL.</li>
 * <li>{@code sslInvalidHostNameAllowed=true|false}: Whether to allow invalid host names for SSL connections.</li>
 * <li>{@code connectTimeoutMS=ms}: How long a connection can take to be opened before timing out.</li>
 * <li>{@code socketTimeoutMS=ms}: How long a send or receive on a socket can take before timing out.</li>
 * <li>{@code maxIdleTimeMS=ms}: Maximum idle time of a pooled connection. A connection that exceeds this limit will be closed</li>
 * <li>{@code maxLifeTimeMS=ms}: Maximum life time of a pooled connection. A connection that exceeds this limit will be closed</li>
 * </ul>
 * <p>Connection pool configuration:</p>
 * <ul>
 * <li>{@code maxPoolSize=n}: The maximum number of connections in the connection pool.</li>
 * <li>{@code waitQueueMultiple=n} : this multiplier, multiplied with the maxPoolSize setting, gives the maximum number of
 * threads that may be waiting for a connection to become available from the pool.  All further threads will get an
 * exception right away.</li>
 * <li>{@code waitQueueTimeoutMS=ms}: The maximum wait time in milliseconds that a thread may wait for a connection to
 * become available.</li>
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
 * <li>{@code retryWrites=true|false}. If true the driver will retry supported write operations if they fail due to a network error.
 *  Defaults to false.</li>
 * <li>{@code wtimeoutMS=ms}
 * <ul>
 * <li>The driver adds { wtimeout : ms } to all write commands. Implies {@code safe=true}.</li>
 * <li>Used in combination with {@code w}</li>
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
 * estimates the staleness of each secondary, based on lastWriteDate values provided in server isMaster responses, and selects only those
 * secondaries whose staleness is less than or equal to maxStalenessSeconds.  Not providing the parameter or explicitly setting it to -1
 * indicates that there should be no max staleness check. The maximum staleness feature is designed to prevent badly-lagging servers from
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
 * currently are 'zlib' and 'snappy'.</li>
 * <li>{@code zlibCompressionLevel=integer}: Integer value from -1 to 9 representing the zlib compression level. Lower values will make
 * compression faster, while higher values will make compression better.</li>
 * </ul>
 *
 * @mongodb.driver.manual reference/connection-string Connection String Format
 * @since 3.0.0
 */
public class ConnectionString {

    private static final String MONGODB_PREFIX = "mongodb://";
    private static final String MONGODB_SRV_PREFIX = "mongodb+srv://";
    private static final Set<String> ALLOWED_OPTIONS_IN_TXT_RECORD = new HashSet<String>(asList("authsource", "replicaset"));
    private static final String UTF_8 = "UTF-8";

    private static final Logger LOGGER = Loggers.getLogger("uri");

    private final MongoCredential credential;
    private final List<String> hosts;
    private final String database;
    private final String collection;
    private final String connectionString;

    private ReadPreference readPreference;
    private WriteConcern writeConcern;
    private boolean retryWrites;
    private ReadConcern readConcern;

    private Integer minConnectionPoolSize;
    private Integer maxConnectionPoolSize;
    private Integer threadsAllowedToBlockForConnectionMultiplier;
    private Integer maxWaitTime;
    private Integer maxConnectionIdleTime;
    private Integer maxConnectionLifeTime;
    private Integer connectTimeout;
    private Integer socketTimeout;
    private Boolean sslEnabled;
    private Boolean sslInvalidHostnameAllowed;
    private String streamType;
    private String requiredReplicaSetName;
    private Integer serverSelectionTimeout;
    private Integer localThreshold;
    private Integer heartbeatFrequency;
    private String applicationName;
    private List<MongoCompressor> compressorList;

    /**
     * Creates a ConnectionString from the given string.
     *
     * @param connectionString     the connection string
     * @since 3.0
     */
    public ConnectionString(final String connectionString) {
        this.connectionString = connectionString;
        boolean isMongoDBProtocol = connectionString.startsWith(MONGODB_PREFIX);
        boolean isSRVProtocol = connectionString.startsWith(MONGODB_SRV_PREFIX);
        if (!isMongoDBProtocol && !isSRVProtocol) {
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
        int idx = unprocessedConnectionString.lastIndexOf("/");
        if (idx == -1) {
            if (unprocessedConnectionString.contains("?")) {
                throw new IllegalArgumentException("The connection string contains options without trailing slash");
            }
            userAndHostInformation = unprocessedConnectionString;
            unprocessedConnectionString = "";
        } else {
            userAndHostInformation = unprocessedConnectionString.substring(0, idx);
            unprocessedConnectionString = unprocessedConnectionString.substring(idx + 1);
        }

        // Split the user and host information
        String userInfo;
        String hostIdentifier;
        String userName = null;
        char[] password = null;
        idx = userAndHostInformation.lastIndexOf("@");
        if (idx > 0) {
            userInfo = userAndHostInformation.substring(0, idx);
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
                userName = urldecode(userInfo.substring(0, idx));
                password = urldecode(userInfo.substring(idx + 1), true).toCharArray();
            }
        } else {
            hostIdentifier = userAndHostInformation;
        }

        // Validate the hosts
        List<String> unresolvedHosts = unmodifiableList(parseHosts(asList(hostIdentifier.split(",")), isSRVProtocol));
        this.hosts = isSRVProtocol ? resolveHostFromSrvRecords(unresolvedHosts.get(0)) : unresolvedHosts;

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
        } else {
            database = null;
            collection = null;
        }

        String txtRecordsQueryParameters = isSRVProtocol ? resolveAdditionalQueryParametersFromTxtRecords(unresolvedHosts.get(0)) : "";
        String connectionStringQueryParamenters = unprocessedConnectionString;

        Map<String, List<String>> connectionStringOptionsMap = parseOptions(connectionStringQueryParamenters);
        Map<String, List<String>> txtRecordsOptionsMap = parseOptions(txtRecordsQueryParameters);
        if (!ALLOWED_OPTIONS_IN_TXT_RECORD.containsAll(txtRecordsOptionsMap.keySet())) {
            throw new MongoConfigurationException(format("A TXT record is only permitted to contain the keys %s, but the TXT record for "
            + "'%s' contains the keys %s", ALLOWED_OPTIONS_IN_TXT_RECORD, unresolvedHosts.get(0), txtRecordsOptionsMap.keySet()));
        }
        Map<String, List<String>> combinedOptionsMaps = combineOptionsMaps(txtRecordsOptionsMap, connectionStringOptionsMap);
        if (isSRVProtocol && !combinedOptionsMaps.containsKey("ssl")) {
            combinedOptionsMaps.put("ssl", singletonList("true"));
        }
        translateOptions(combinedOptionsMaps);
        credential = createCredentials(combinedOptionsMaps, userName, password);
        warnOnUnsupportedOptions(combinedOptionsMaps);
    }

    private static final Set<String> GENERAL_OPTIONS_KEYS = new HashSet<String>();
    private static final Set<String> AUTH_KEYS = new HashSet<String>();
    private static final Set<String> READ_PREFERENCE_KEYS = new HashSet<String>();
    private static final Set<String> WRITE_CONCERN_KEYS = new HashSet<String>();
    private static final Set<String> COMPRESSOR_KEYS = new HashSet<String>();
    private static final Set<String> ALL_KEYS = new HashSet<String>();

    static {
        GENERAL_OPTIONS_KEYS.add("minpoolsize");
        GENERAL_OPTIONS_KEYS.add("maxpoolsize");
        GENERAL_OPTIONS_KEYS.add("waitqueuemultiple");
        GENERAL_OPTIONS_KEYS.add("waitqueuetimeoutms");
        GENERAL_OPTIONS_KEYS.add("connecttimeoutms");
        GENERAL_OPTIONS_KEYS.add("maxidletimems");
        GENERAL_OPTIONS_KEYS.add("maxlifetimems");
        GENERAL_OPTIONS_KEYS.add("sockettimeoutms");
        GENERAL_OPTIONS_KEYS.add("sockettimeoutms");
        GENERAL_OPTIONS_KEYS.add("ssl");
        GENERAL_OPTIONS_KEYS.add("streamtype");
        GENERAL_OPTIONS_KEYS.add("sslinvalidhostnameallowed");
        GENERAL_OPTIONS_KEYS.add("replicaset");
        GENERAL_OPTIONS_KEYS.add("readconcernlevel");

        GENERAL_OPTIONS_KEYS.add("serverselectiontimeoutms");
        GENERAL_OPTIONS_KEYS.add("localthresholdms");
        GENERAL_OPTIONS_KEYS.add("heartbeatfrequencyms");
        GENERAL_OPTIONS_KEYS.add("retrywrites");

        GENERAL_OPTIONS_KEYS.add("appname");

        COMPRESSOR_KEYS.add("compressors");
        COMPRESSOR_KEYS.add("zlibcompressionlevel");

        READ_PREFERENCE_KEYS.add("readpreference");
        READ_PREFERENCE_KEYS.add("readpreferencetags");
        READ_PREFERENCE_KEYS.add("maxstalenessseconds");

        WRITE_CONCERN_KEYS.add("safe");
        WRITE_CONCERN_KEYS.add("w");
        WRITE_CONCERN_KEYS.add("wtimeoutms");
        WRITE_CONCERN_KEYS.add("fsync");
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
        Map<String, List<String>> combinedOptionsMaps = new HashMap<String, List<String>>(txtRecordsOptionsMap);
        for (Map.Entry<String, List<String>> entry : connectionStringOptionsMap.entrySet()) {
            combinedOptionsMaps.put(entry.getKey(), entry.getValue());
        }
        return combinedOptionsMaps;
    }


    private void warnOnUnsupportedOptions(final Map<String, List<String>> optionsMap) {
        for (final String key : optionsMap.keySet()) {
            if (!ALL_KEYS.contains(key)) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Unsupported option '%s' in the connection string '%s'.", key, connectionString));
                }
            }
        }
    }

    private void translateOptions(final Map<String, List<String>> optionsMap) {
        for (final String key : GENERAL_OPTIONS_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("maxpoolsize")) {
                maxConnectionPoolSize = parseInteger(value, "maxpoolsize");
            } else if (key.equals("minpoolsize")) {
                minConnectionPoolSize = parseInteger(value, "minpoolsize");
            } else if (key.equals("maxidletimems")) {
                maxConnectionIdleTime = parseInteger(value, "maxidletimems");
            } else if (key.equals("maxlifetimems")) {
                maxConnectionLifeTime = parseInteger(value, "maxlifetimems");
            } else if (key.equals("waitqueuemultiple")) {
                threadsAllowedToBlockForConnectionMultiplier  = parseInteger(value, "waitqueuemultiple");
            } else if (key.equals("waitqueuetimeoutms")) {
                maxWaitTime = parseInteger(value, "waitqueuetimeoutms");
            } else if (key.equals("connecttimeoutms")) {
                connectTimeout = parseInteger(value, "connecttimeoutms");
            } else if (key.equals("sockettimeoutms")) {
                socketTimeout = parseInteger(value, "sockettimeoutms");
            } else if (key.equals("sslinvalidhostnameallowed")) {
                sslInvalidHostnameAllowed = parseBoolean(value, "sslinvalidhostnameallowed");
            } else if (key.equals("ssl")) {
                sslEnabled = parseBoolean(value, "ssl");
            } else if (key.equals("streamtype")) {
                streamType = value;
            } else if (key.equals("replicaset")) {
                requiredReplicaSetName = value;
            } else if (key.equals("readconcernlevel")) {
                readConcern = new ReadConcern(ReadConcernLevel.fromString(value));
            } else if (key.equals("serverselectiontimeoutms")) {
                serverSelectionTimeout = parseInteger(value, "serverselectiontimeoutms");
            } else if (key.equals("localthresholdms")) {
                localThreshold = parseInteger(value, "localthresholdms");
            } else if (key.equals("heartbeatfrequencyms")) {
                heartbeatFrequency = parseInteger(value, "heartbeatfrequencyms");
            } else if (key.equals("appname")) {
                applicationName = value;
            } else if (key.equals("retrywrites")) {
                retryWrites = parseBoolean(value, "retrywrites");
            }
        }

        writeConcern = createWriteConcern(optionsMap);
        readPreference = createReadPreference(optionsMap);
        compressorList = createCompressors(optionsMap);
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

    private List<MongoCompressor> buildCompressors(final String compressors, final Integer zlibCompressionLevel) {
        List<MongoCompressor> compressorsList = new ArrayList<MongoCompressor>();

        for (String cur : compressors.split(",")) {
            if (cur.equals("zlib")) {
                MongoCompressor zlibCompressor = MongoCompressor.createZlibCompressor();
                if (zlibCompressionLevel != null) {
                    zlibCompressor = zlibCompressor.withProperty(MongoCompressor.LEVEL, zlibCompressionLevel);
                }
                compressorsList.add(zlibCompressor);
            } else if (cur.equals("snappy")) {
                compressorsList.add(MongoCompressor.createSnappyCompressor());
            } else if (!cur.isEmpty()) {
                throw new IllegalArgumentException("Unsupported compressor '" + cur + "'");
            }
        }

        return unmodifiableList(compressorsList);
    }

    private WriteConcern createWriteConcern(final Map<String, List<String>> optionsMap) {
        Boolean safe = null;
        String w = null;
        Integer wTimeout = null;
        Boolean fsync = null;
        Boolean journal = null;

        for (final String key : WRITE_CONCERN_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("safe")) {
                safe = parseBoolean(value, "safe");
            } else if (key.equals("w")) {
                w = value;
            } else if (key.equals("wtimeoutms")) {
                wTimeout = Integer.parseInt(value);
            } else if (key.equals("fsync")) {
                fsync = parseBoolean(value, "fsync");
            } else if (key.equals("journal")) {
                journal = parseBoolean(value, "journal");
            }
        }
        return buildWriteConcern(safe, w, wTimeout, fsync, journal);
    }

    private ReadPreference createReadPreference(final Map<String, List<String>> optionsMap) {
        String readPreferenceType = null;
        List<TagSet> tagSetList = new ArrayList<TagSet>();
        long maxStalenessSeconds = -1;

        for (final String key : READ_PREFERENCE_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("readpreference")) {
                readPreferenceType = value;
            } else if (key.equals("maxstalenessseconds")) {
                 maxStalenessSeconds = parseInteger(value, "maxstalenessseconds");
            } else if (key.equals("readpreferencetags")) {
                for (final String cur : optionsMap.get(key)) {
                    TagSet tagSet = getTags(cur.trim());
                    tagSetList.add(tagSet);
                }
            }
        }
        return buildReadPreference(readPreferenceType, tagSetList, maxStalenessSeconds);
    }

    private MongoCredential createCredentials(final Map<String, List<String>> optionsMap, final String userName,
                                              final char[] password) {
        AuthenticationMechanism mechanism = null;
        String authSource = (database == null) ? "admin" : database;
        String gssapiServiceName = null;
        String authMechanismProperties = null;

        for (final String key : AUTH_KEYS) {
            String value = getLastValue(optionsMap, key);

            if (value == null) {
                continue;
            }

            if (key.equals("authmechanism")) {
                mechanism = AuthenticationMechanism.fromMechanismName(value);
            } else if (key.equals("authsource")) {
                authSource = value;
            } else if (key.equals("gssapiservicename")) {
                gssapiServiceName = value;
            } else if (key.equals("authmechanismproperties")) {
                authMechanismProperties = value;
            }
        }


        MongoCredential credential = null;
        if (mechanism != null) {
            switch (mechanism) {
                case GSSAPI:
                    credential = MongoCredential.createGSSAPICredential(userName);
                    if (gssapiServiceName != null) {
                        credential = credential.withMechanismProperty("SERVICE_NAME", gssapiServiceName);
                    }
                    break;
                case PLAIN:
                    credential = MongoCredential.createPlainCredential(userName, authSource, password);
                    break;
                case MONGODB_CR:
                    credential = MongoCredential.createMongoCRCredential(userName, authSource, password);
                    break;
                case MONGODB_X509:
                    credential = MongoCredential.createMongoX509Credential(userName);
                    break;
                case SCRAM_SHA_1:
                    credential = MongoCredential.createScramSha1Credential(userName, authSource, password);
                    break;
                default:
                    throw new UnsupportedOperationException(format("The connection string contains an invalid authentication mechanism'. "
                                                                           + "'%s' is not a supported authentication mechanism",
                            mechanism));
            }
        } else if (userName != null) {
            credential = MongoCredential.createCredential(userName, authSource, password);
        }

        if (credential != null && authMechanismProperties != null) {
            for (String part : authMechanismProperties.split(",")) {
                String[] mechanismPropertyKeyValue = part.split(":");
                if (mechanismPropertyKeyValue.length != 2) {
                    throw new IllegalArgumentException(format("The connection string contains invalid authentication properties. "
                            + "'%s' is not a key value pair", part));
                }
                String key = mechanismPropertyKeyValue[0].trim().toLowerCase();
                String value = mechanismPropertyKeyValue[1].trim();
                if (key.equals("canonicalize_host_name")) {
                    credential = credential.withMechanismProperty(key, Boolean.valueOf(value));
                } else {
                    credential = credential.withMechanismProperty(key, value);
                }
            }
        }
        return credential;
    }

    private String getLastValue(final Map<String, List<String>> optionsMap, final String key) {
        List<String> valueList = optionsMap.get(key);
        if (valueList == null) {
            return null;
        }
        return valueList.get(valueList.size() - 1);
    }

    private Map<String, List<String>> parseOptions(final String optionsPart) {
        Map<String, List<String>> optionsMap = new HashMap<String, List<String>>();
        if (optionsPart.length() == 0) {
            return optionsMap;
        }

        for (final String part : optionsPart.split("&|;")) {
            if (part.length() == 0) {
                continue;
            }
            int idx = part.indexOf("=");
            if (idx >= 0) {
                String key = part.substring(0, idx).toLowerCase();
                String value = part.substring(idx + 1);
                List<String> valueList = optionsMap.get(key);
                if (valueList == null) {
                    valueList = new ArrayList<String>(1);
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
        // handle legacy slaveok settings
        if (optionsMap.containsKey("slaveok") && !optionsMap.containsKey("readpreference")) {
            String readPreference = parseBoolean(getLastValue(optionsMap, "slaveok"), "slaveok")
                                    ? "secondaryPreferred" : "primary";
            optionsMap.put("readpreference", singletonList(readPreference));
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Uri option 'slaveok' has been deprecated, use 'readpreference' instead.");
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

    private ReadPreference buildReadPreference(final String readPreferenceType,
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

    @SuppressWarnings("deprecation")
    private WriteConcern buildWriteConcern(final Boolean safe, final String w,
                                           final Integer wTimeout, final Boolean fsync, final Boolean journal) {
        WriteConcern retVal = null;
        if (w != null || wTimeout != null || fsync != null || journal != null) {
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
            if (fsync != null) {
                retVal = retVal.withFsync(fsync);
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
        List<Tag> tagList = new ArrayList<Tag>();
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

    private boolean parseBoolean(final String input, final String key) {
        String trimmedInput = input.trim();
        boolean isTrue = trimmedInput.length() > 0 && (trimmedInput.equals("1") || trimmedInput.toLowerCase().equals("true")
                || trimmedInput.toLowerCase().equals("yes"));

        if ((!input.equals("true") && !input.equals("false")) && LOGGER.isWarnEnabled()) {
            LOGGER.warn(format("Deprecated boolean value ('%s') in the connection string for '%s', please update to %s=%s",
                    input, key, key, isTrue));
        }
        return isTrue;
    }

    private int parseInteger(final String input, final String key) {
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(format("The connection string contains an invalid value for '%s'. "
                    + "'%s' is not a valid integer", key, input));
        }
    }

    private List<String> parseHosts(final List<String> rawHosts, final boolean isSRVProtocol) {
        if (rawHosts.size() == 0){
            throw new IllegalArgumentException("The connection string must contain at least one host");
        }
        List<String> hosts = new ArrayList<String>();
        for (String host : rawHosts) {
            if (host.length() == 0) {
                throw new IllegalArgumentException(format("The connection string contains an empty host '%s'. ", rawHosts));
            } else if (host.endsWith(".sock")) {
                throw new IllegalArgumentException(format("The connection string contains an invalid host '%s'. "
                        + "Unix Domain Socket which is not supported by the Java driver", host));
            } else if (host.startsWith("[")) {
                if (!host.contains("]")) {
                    throw new IllegalArgumentException(format("The connection string contains an invalid host '%s'. "
                            + "IPv6 address literals must be enclosed in '[' and ']' according to RFC 2732", host));
                }
                int idx = host.indexOf("]:");
                if (idx != -1) {
                    validatePort(host, host.substring(idx + 2));
                }
            } else {
                int colonCount = countOccurrences(host, ":");
                if (colonCount > 1) {
                    throw new IllegalArgumentException(format("The connection string contains an invalid host '%s'. "
                            + "Reserved characters such as ':' must be escaped according RFC 2396. "
                            + "Any IPv6 address literal must be enclosed in '[' and ']' according to RFC 2732.", host));
                } else if (colonCount == 1) {
                    if (isSRVProtocol) {
                        throw new IllegalArgumentException("A connection string using the mongodb+srv protocol can not"
                                + "contain a host name that specifies a port");
                    }

                    validatePort(host, host.substring(host.indexOf(":") + 1));
                }
            }
            hosts.add(host);
        }
        if (isSRVProtocol && hosts.size() > 1) {
            throw new IllegalArgumentException("The mongodb+srv protocol requires a single host name but this connection string has more "
                    + "than one: " + connectionString);
        }
        Collections.sort(hosts);
        return hosts;
    }

    private void validatePort(final String host, final String port) {
        boolean invalidPort = false;
        try {
            int portInt = Integer.parseInt(port);
            if (portInt <= 0 || portInt > 65535) {
                invalidPort = true;
            }
        } catch (NumberFormatException e) {
            invalidPort = true;
        }
        if (invalidPort) {
            throw new IllegalArgumentException(format("The connection string contains an invalid host '%s'. "
                    + "The port '%s' is not a valid, it must be an integer between 0 and 65535", host, port));
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
            return URLDecoder.decode(input, UTF_8);
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
    public String getUsername() {
        return credential != null ? credential.getUserName() : null;
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    public char[] getPassword() {
        return credential != null ? credential.getPassword() : null;
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
    public String getDatabase() {
        return database;
    }


    /**
     * Gets the collection name
     *
     * @return the collection name
     */
    public String getCollection() {
        return collection;
    }

    /**
     * Get the unparsed connection string.
     *
     * @return the connection string
     * deprecated use {@link #getConnectionString()}
     */
    @Deprecated
    public String getURI() {
        return getConnectionString();
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
     * Gets the credentials in an immutable list.  The list will be empty if no credentials were specified in the connection string.
     *
     * @return the credentials in an immutable list
     * @deprecated Prefer {@link #getCredential()}
     */
    @Deprecated
    public List<MongoCredential> getCredentialList() {
        return credential != null ? singletonList(credential) : Collections.<MongoCredential>emptyList();
    }

    /**
     * Gets the credential or null if no credentials were specified in the connection string.
     *
     * @return the credentials in an immutable list
     * @since 3.6
     */
    public MongoCredential getCredential() {
        return credential;
    }

    /**
     * Gets the read preference specified in the connection string.
     * @return the read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets the read concern specified in the connection string.
     * @return the read concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * Gets the write concern specified in the connection string.
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Returns true if writes should be retried if they fail due to a network error.
     *
     * @return the retryWrites value
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public boolean getRetryWrites() {
        return retryWrites;
    }

    /**
     * Gets the minimum connection pool size specified in the connection string.
     * @return the minimum connection pool size
     */
    public Integer getMinConnectionPoolSize() {
        return minConnectionPoolSize;
    }

    /**
     * Gets the maximum connection pool size specified in the connection string.
     * @return the maximum connection pool size
     */
    public Integer getMaxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    /**
     * Gets the multiplier for the number of threads allowed to block waiting for a connection specified in the connection string.
     * @return the multiplier for the number of threads allowed to block waiting for a connection
     */
    public Integer getThreadsAllowedToBlockForConnectionMultiplier() {
        return threadsAllowedToBlockForConnectionMultiplier;
    }

    /**
     * Gets the maximum wait time of a thread waiting for a connection specified in the connection string.
     * @return the maximum wait time of a thread waiting for a connection
     */
    public Integer getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     * Gets the maximum connection idle time specified in the connection string.
     * @return the maximum connection idle time
     */
    public Integer getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    /**
     * Gets the maximum connection life time specified in the connection string.
     * @return the maximum connection life time
     */
    public Integer getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }

    /**
     * Gets the socket connect timeout specified in the connection string.
     * @return the socket connect timeout
     */
    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Gets the socket timeout specified in the connection string.
     * @return the socket timeout
     */
    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * Gets the SSL enabled value specified in the connection string.
     * @return the SSL enabled value
     */
    public Boolean getSslEnabled() {
        return sslEnabled;
    }

    /**
     * Gets the stream type value specified in the connection string.
     * @return the stream type value
     * @since 3.3
     */
    public String getStreamType() {
        return streamType;
    }

    /**
     * Gets the SSL invalidHostnameAllowed value specified in the connection string.
     *
     * @return the SSL invalidHostnameAllowed value
     * @since 3.3
     */
    public Boolean getSslInvalidHostnameAllowed() {
        return sslInvalidHostnameAllowed;
    }

    /**
     * Gets the required replica set name specified in the connection string.
     * @return the required replica set name
     */
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    /**
     *
     * @return the server selection timeout (in milliseconds), or null if unset
     * @since 3.3
     */
    public Integer getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }

    /**
     *
     * @return the local threshold (in milliseconds), or null if unset
     * since 3.3
     */
    public Integer getLocalThreshold() {
        return localThreshold;
    }

    /**
     *
     * @return the heartbeat frequency (in milliseconds), or null if unset
     * since 3.3
     */
    public Integer getHeartbeatFrequency() {
        return heartbeatFrequency;
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

    @Override
    public String toString() {
        return connectionString;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectionString)) {
            return false;
        }

        ConnectionString that = (ConnectionString) o;

        if (collection != null ? !collection.equals(that.collection) : that.collection != null) {
            return false;
        }
        if (connectTimeout != null ? !connectTimeout.equals(that.connectTimeout) : that.connectTimeout != null) {
            return false;
        }
        if (credential != null ? !credential.equals(that.credential) : that.credential != null) {
            return false;
        }
        if (database != null ? !database.equals(that.database) : that.database != null) {
            return false;
        }
        if (!hosts.equals(that.hosts)) {
            return false;
        }
        if (maxConnectionIdleTime != null ? !maxConnectionIdleTime.equals(that.maxConnectionIdleTime)
                                          : that.maxConnectionIdleTime != null) {
            return false;
        }
        if (maxConnectionLifeTime != null ? !maxConnectionLifeTime.equals(that.maxConnectionLifeTime)
                                          : that.maxConnectionLifeTime != null) {
            return false;
        }
        if (maxConnectionPoolSize != null ? !maxConnectionPoolSize.equals(that.maxConnectionPoolSize)
                                          : that.maxConnectionPoolSize != null) {
            return false;
        }
        if (maxWaitTime != null ? !maxWaitTime.equals(that.maxWaitTime) : that.maxWaitTime != null) {
            return false;
        }
        if (minConnectionPoolSize != null ? !minConnectionPoolSize.equals(that.minConnectionPoolSize)
                                          : that.minConnectionPoolSize != null) {
            return false;
        }
        if (readPreference != null ? !readPreference.equals(that.readPreference) : that.readPreference != null) {
            return false;
        }
        if (requiredReplicaSetName != null ? !requiredReplicaSetName.equals(that.requiredReplicaSetName)
                                           : that.requiredReplicaSetName != null) {
            return false;
        }
        if (socketTimeout != null ? !socketTimeout.equals(that.socketTimeout) : that.socketTimeout != null) {
            return false;
        }
        if (sslEnabled != null ? !sslEnabled.equals(that.sslEnabled) : that.sslEnabled != null) {
            return false;
        }
        if (threadsAllowedToBlockForConnectionMultiplier != null
            ? !threadsAllowedToBlockForConnectionMultiplier.equals(that.threadsAllowedToBlockForConnectionMultiplier)
            : that.threadsAllowedToBlockForConnectionMultiplier != null) {
            return false;
        }
        if (writeConcern != null ? !writeConcern.equals(that.writeConcern) : that.writeConcern != null) {
            return false;
        }
        if (applicationName != null ? !applicationName.equals(that.applicationName) : that.applicationName != null) {
            return false;
        }
        if (!compressorList.equals(that.compressorList)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = credential != null ? credential.hashCode() : 0;
        result = 31 * result + hosts.hashCode();
        result = 31 * result + (database != null ? database.hashCode() : 0);
        result = 31 * result + (collection != null ? collection.hashCode() : 0);
        result = 31 * result + (readPreference != null ? readPreference.hashCode() : 0);
        result = 31 * result + (writeConcern != null ? writeConcern.hashCode() : 0);
        result = 31 * result + (minConnectionPoolSize != null ? minConnectionPoolSize.hashCode() : 0);
        result = 31 * result + (maxConnectionPoolSize != null ? maxConnectionPoolSize.hashCode() : 0);
        result = 31 * result + (threadsAllowedToBlockForConnectionMultiplier != null
                                ? threadsAllowedToBlockForConnectionMultiplier.hashCode()
                                : 0);
        result = 31 * result + (maxWaitTime != null ? maxWaitTime.hashCode() : 0);
        result = 31 * result + (maxConnectionIdleTime != null ? maxConnectionIdleTime.hashCode() : 0);
        result = 31 * result + (maxConnectionLifeTime != null ? maxConnectionLifeTime.hashCode() : 0);
        result = 31 * result + (connectTimeout != null ? connectTimeout.hashCode() : 0);
        result = 31 * result + (socketTimeout != null ? socketTimeout.hashCode() : 0);
        result = 31 * result + (sslEnabled != null ? sslEnabled.hashCode() : 0);
        result = 31 * result + (requiredReplicaSetName != null ? requiredReplicaSetName.hashCode() : 0);
        result = 31 * result + (applicationName != null ? applicationName.hashCode() : 0);
        result = 31 * result + compressorList.hashCode();
        return result;
    }
}
