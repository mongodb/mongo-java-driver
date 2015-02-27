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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.mongodb.AuthenticationMechanism.GSSAPI;
import static com.mongodb.AuthenticationMechanism.MONGODB_CR;
import static com.mongodb.AuthenticationMechanism.MONGODB_X509;
import static com.mongodb.AuthenticationMechanism.PLAIN;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static java.lang.String.format;
import static java.util.Arrays.asList;


/**
 * <p>Represents a <a href="http://www.mongodb.org/display/DOCS/Connections">URI</a>. The URI describes the hosts to
 * be used and options.</p>
 *
 * <p>The format of the URI is:</p>
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
 *
 * <p>The following options are supported (case insensitive):</p>
 *
 * <p>Replica set configuration:</p>
 * <ul>
 * <li>{@code replicaSet=name}: Implies that the hosts given are a seed list, and the driver will attempt to find
 * all members of the set.</li>
 * </ul>
 * <p>Connection Configuration:</p>
 * <ul>
 * <li>{@code ssl=true|false}: Whether to connect using SSL.</li>
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
 * <li>{@code true}: the driver sends a getLastError command after every update to ensure that the update succeeded
 * (see also {@code w} and {@code wtimeoutMS}).</li>
 * <li>{@code false}: the driver does not send a getLastError command after every update.</li>
 * </ul>
 * </li>
 * <li>{@code w=wValue}
 * <ul>
 * <li>The driver adds { w : wValue } to the getLastError command. Implies {@code safe=true}.</li>
 * <li>wValue is typically a number, but can be any string in order to allow for specifications like
 * {@code "majority"}</li>
 * </ul>
 * </li>
 * <li>{@code wtimeoutMS=ms}
 * <ul>
 * <li>The driver adds { wtimeout : ms } to the getlasterror command. Implies {@code safe=true}.</li>
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
 * <li>{@code authMechanismProperties=PROPERTY_NAME:PROPERTY_VALUE,PROPERTY_NAME2:PROPERTY_VALUE2}: This option allows authentication
 * mechanism properties to be set on the connection string.
 * </li>
 * <li>{@code gssapiServiceName=string}: This option only applies to the GSSAPI mechanism and is used to alter the service name.
 *   Deprecated, please use {@code authMechanismProperties=SERVICE_NAME:string} instead.
 * </li>
 * </ul>
 *
 * @mongodb.driver.manual reference/connection-string Connection String URI Format
 * @since 3.0.0
 */
public class ConnectionString {

    private static final String PREFIX = "mongodb://";
    private static final String UTF_8 = "UTF-8";

    private static final Logger LOGGER = Loggers.getLogger("uri");

    private final MongoCredential credentials;
    private final List<String> hosts;
    private final String database;
    private final String collection;
    private final String uri;

    private ReadPreference readPreference;
    private WriteConcern writeConcern;

    private Integer minConnectionPoolSize;
    private Integer maxConnectionPoolSize;
    private Integer threadsAllowedToBlockForConnectionMultiplier;
    private Integer maxWaitTime;
    private Integer maxConnectionIdleTime;
    private Integer maxConnectionLifeTime;
    private Integer connectTimeout;
    private Integer socketTimeout;
    private Boolean sslEnabled;
    private String requiredReplicaSetName;

    /**
     * Creates a MongoURI from the given URI string, and MongoClientOptions.Builder.  The builder can be configured with default options,
     * which may be overridden by options specified in the URI string.
     *
     * @param uri     the URI
     * @since 2.11.0
     */
    public ConnectionString(final String uri) {
        try {
            if (!uri.startsWith(PREFIX)) {
                throw new IllegalArgumentException("uri needs to start with " + PREFIX);
            }

            this.uri = uri;

            String unprefixedURI = uri.substring(PREFIX.length());

            String serverPart;
            String nsPart;
            String optionsPart;
            String userName = null;
            char[] password = null;

            int idx = unprefixedURI.lastIndexOf("/");
            if (idx < 0) {
                if (unprefixedURI.contains("?")) {
                    throw new IllegalArgumentException("URI contains options without trailing slash");
                }
                serverPart = unprefixedURI;
                nsPart = null;
                optionsPart = "";
            } else {
                serverPart = unprefixedURI.substring(0, idx);
                nsPart = unprefixedURI.substring(idx + 1);

                idx = nsPart.indexOf("?");
                if (idx >= 0) {
                    optionsPart = nsPart.substring(idx + 1);
                    nsPart = nsPart.substring(0, idx);
                } else {
                    optionsPart = "";
                }

            }
            List<String> all = new LinkedList<String>();

            idx = serverPart.indexOf("@");

            if (idx > 0) {
                String authPart = serverPart.substring(0, idx);
                serverPart = serverPart.substring(idx + 1);

                idx = authPart.indexOf(":");
                if (idx == -1) {
                    userName = URLDecoder.decode(authPart, UTF_8);
                } else {
                    userName = URLDecoder.decode(authPart.substring(0, idx), UTF_8);
                    password = URLDecoder.decode(authPart.substring(idx + 1), UTF_8).toCharArray();
                }
            }

            Collections.addAll(all, serverPart.split(","));

            Collections.sort(all);
            hosts = Collections.unmodifiableList(all);

            if (nsPart != null && !nsPart.isEmpty()) { // database,_collection
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

            Map<String, List<String>> optionsMap = parseOptions(optionsPart);
            translateOptions(optionsMap);
            credentials = createCredentials(optionsMap, userName, password);
            warnOnUnsupportedOptions(optionsMap);
        } catch (UnsupportedEncodingException e) {
            throw new MongoInternalException("This should not happen", e);
        }
    }

    private static final Set<String> GENERAL_OPTIONS_KEYS = new HashSet<String>();
    private static final Set<String> AUTH_KEYS = new HashSet<String>();
    private static final Set<String> READ_PREFERENCE_KEYS = new HashSet<String>();
    private static final Set<String> WRITE_CONCERN_KEYS = new HashSet<String>();
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
        GENERAL_OPTIONS_KEYS.add("replicaset");

        READ_PREFERENCE_KEYS.add("readpreference");
        READ_PREFERENCE_KEYS.add("readpreferencetags");

        WRITE_CONCERN_KEYS.add("safe");
        WRITE_CONCERN_KEYS.add("w");
        WRITE_CONCERN_KEYS.add("wtimeoutms");
        WRITE_CONCERN_KEYS.add("fsync");
        WRITE_CONCERN_KEYS.add("j");

        AUTH_KEYS.add("authmechanism");
        AUTH_KEYS.add("authsource");
        AUTH_KEYS.add("gssapiservicename");
        AUTH_KEYS.add("authmechanismproperties");

        ALL_KEYS.addAll(GENERAL_OPTIONS_KEYS);
        ALL_KEYS.addAll(AUTH_KEYS);
        ALL_KEYS.addAll(READ_PREFERENCE_KEYS);
        ALL_KEYS.addAll(WRITE_CONCERN_KEYS);
    }

    private void warnOnUnsupportedOptions(final Map<String, List<String>> optionsMap) {
        for (final String key : optionsMap.keySet()) {
            if (!ALL_KEYS.contains(key)) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn(format("Unsupported option '%s' on URI '%s'.", key, uri));
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
                maxConnectionPoolSize = Integer.parseInt(value);
            } else if (key.equals("minpoolsize")) {
                minConnectionPoolSize = Integer.parseInt(value);
            } else if (key.equals("maxidletimems")) {
                maxConnectionIdleTime = Integer.parseInt(value);
            } else if (key.equals("maxlifetimems")) {
                maxConnectionLifeTime = Integer.parseInt(value);
            } else if (key.equals("waitqueuemultiple")) {
                threadsAllowedToBlockForConnectionMultiplier  = Integer.parseInt(value);
            } else if (key.equals("waitqueuetimeoutms")) {
                maxWaitTime = Integer.parseInt(value);
            } else if (key.equals("connecttimeoutms")) {
                connectTimeout = Integer.parseInt(value);
            } else if (key.equals("sockettimeoutms")) {
                socketTimeout = Integer.parseInt(value);
            } else if (key.equals("ssl") && parseBoolean(value)) {
                sslEnabled = true;
            } else if (key.equals("replicaset")) {
                requiredReplicaSetName = value;
            }
        }

        writeConcern = createWriteConcern(optionsMap);
        readPreference = createReadPreference(optionsMap);
    }

    private WriteConcern createWriteConcern(final Map<String, List<String>> optionsMap) {
        Boolean safe = null;
        String w = null;
        int wTimeout = 0;
        boolean fsync = false;
        boolean journal = false;

        for (final String key : WRITE_CONCERN_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("safe")) {
                safe = parseBoolean(value);
            } else if (key.equals("w")) {
                w = value;
            } else if (key.equals("wtimeoutms")) {
                wTimeout = Integer.parseInt(value);
            } else if (key.equals("fsync")) {
                fsync = parseBoolean(value);
            } else if (key.equals("j")) {
                journal = parseBoolean(value);
            }
        }
        return buildWriteConcern(safe, w, wTimeout, fsync, journal);
    }

    private ReadPreference createReadPreference(final Map<String, List<String>> optionsMap) {
        String readPreferenceType = null;
        List<TagSet> tagSetList = new ArrayList<TagSet>();

        for (final String key : READ_PREFERENCE_KEYS) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("readpreference")) {
                readPreferenceType = value;
            } else if (key.equals("readpreferencetags")) {
                for (final String cur : optionsMap.get(key)) {
                    TagSet tagSet = getTags(cur.trim());
                    tagSetList.add(tagSet);
                }
            }
        }
        return buildReadPreference(readPreferenceType, tagSetList);
    }

    private MongoCredential createCredentials(final Map<String, List<String>> optionsMap, final String userName,
                                              final char[] password) {
        if (userName == null) {
            return null;
        }

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
            } else if (key.endsWith("authmechanismproperties")) {
                authMechanismProperties = value;
            }
        }

        MongoCredential credential;
        if (mechanism == GSSAPI) {
            credential = MongoCredential.createGSSAPICredential(userName);
            if (gssapiServiceName != null) {
                credential = credential.withMechanismProperty("SERVICE_NAME", gssapiServiceName);
            }
        } else if (mechanism == PLAIN) {
            credential = MongoCredential.createPlainCredential(userName, authSource, password);
        } else if (mechanism == MONGODB_CR) {
            credential = MongoCredential.createMongoCRCredential(userName, authSource, password);
        } else if (mechanism == MONGODB_X509) {
            credential = MongoCredential.createMongoX509Credential(userName);
        } else if (mechanism == SCRAM_SHA_1) {
            credential = MongoCredential.createScramSha1Credential(userName, authSource, password);
        } else if (mechanism == null) {
            credential = MongoCredential.createCredential(userName, authSource, password);
        } else {
            throw new UnsupportedOperationException("Unsupported authentication mechanism in the URI: " + mechanism);
        }

        if (authMechanismProperties != null) {
            for (String part : authMechanismProperties.split(",")) {
                String[] mechanismPropertyKeyValue = part.split(":");
                if (mechanismPropertyKeyValue.length != 2) {
                    throw new IllegalArgumentException("Bad authMechanismProperties: " + authMechanismProperties);
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

        for (final String part : optionsPart.split("&|;")) {
            int idx = part.indexOf("=");
            if (idx >= 0) {
                String key = part.substring(0, idx).toLowerCase();
                String value = part.substring(idx + 1);
                List<String> valueList = optionsMap.get(key);
                if (valueList == null) {
                    valueList = new ArrayList<String>(1);
                }
                valueList.add(value);
                optionsMap.put(key, valueList);
            }
        }

        // handle legacy wtimeout settings
        if (optionsMap.containsKey("wtimeout") && !optionsMap.containsKey("wtimeoutms")) {
            optionsMap.put("wtimeoutms", optionsMap.remove("wtimeout"));
        }
        // handle legacy slaveok settings
        if (optionsMap.containsKey("slaveok") && !optionsMap.containsKey("readpreference")) {
            optionsMap.put("readpreference", asList("secondaryPreferred"));
        }

        return optionsMap;
    }

    private ReadPreference buildReadPreference(final String readPreferenceType,
                                               final List<TagSet> tagSetList) {
        if (readPreferenceType != null) {
            if (tagSetList.isEmpty()) {
                return ReadPreference.valueOf(readPreferenceType);
            }
            return ReadPreference.valueOf(readPreferenceType, tagSetList);
        }
        return null;
    }

    private WriteConcern buildWriteConcern(final Boolean safe, final String w,
                                           final int wTimeout, final boolean fsync, final boolean journal) {
        if (w != null || wTimeout != 0 || fsync || journal) {
            if (w == null) {
                return new WriteConcern(1, wTimeout, fsync, journal);
            } else {
                try {
                    return new WriteConcern(Integer.parseInt(w), wTimeout, fsync, journal);
                } catch (NumberFormatException e) {
                    return new WriteConcern(w, wTimeout, fsync, journal);
                }
            }
        } else if (safe != null) {
            if (safe) {
                return WriteConcern.ACKNOWLEDGED;
            } else {
                return WriteConcern.UNACKNOWLEDGED;
            }
        }
        return null;
    }

    private TagSet getTags(final String tagSetString) {
        List<Tag> tagList = new ArrayList<Tag>();
        if (tagSetString.length() > 0) {
            for (final String tag : tagSetString.split(",")) {
                String[] tagKeyValuePair = tag.split(":");
                if (tagKeyValuePair.length != 2) {
                    throw new IllegalArgumentException("Bad read preference tags: " + tagSetString);
                }
                tagList.add(new Tag(tagKeyValuePair[0].trim(), tagKeyValuePair[1].trim()));
            }
        }
        return new TagSet(tagList);
    }

    boolean parseBoolean(final String input) {
        String trimmedInput = input.trim();
        return trimmedInput.length() > 0
               && (trimmedInput.equals("1") || trimmedInput.toLowerCase().equals("true") || trimmedInput.toLowerCase().equals("yes"));
    }

    // ---------------------------------

    /**
     * Gets the username
     *
     * @return the username
     */
    public String getUsername() {
        return credentials != null ? credentials.getUserName() : null;
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    public char[] getPassword() {
        return credentials != null ? credentials.getPassword() : null;
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
     * Get the unparsed URI.
     *
     * @return the URI
     */
    public String getURI() {
        return uri;
    }


    /**
     * Gets the credentials.
     *
     * @return the credentials
     */
    public List<MongoCredential> getCredentialList() {
        return credentials != null ? asList(credentials) : new ArrayList<MongoCredential>();
    }

    /**
     * Gets the read preference specified in the connection string.
     * @return the read preference
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets the write concern specified in the connection string.
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
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
     * Gets the required replica set name specified in the connection string.
     * @return the required replica set name
     */
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    @Override
    public String toString() {
        return uri;
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
        if (credentials != null ? !credentials.equals(that.credentials) : that.credentials != null) {
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

        return true;
    }

    @Override
    public int hashCode() {
        int result = credentials != null ? credentials.hashCode() : 0;
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
        return result;
    }
}
