/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import javax.net.ssl.SSLSocketFactory;
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
import java.util.logging.Logger;


/**
 * Represents a <a href="http://www.mongodb.org/display/DOCS/Connections">URI</a>
 * which can be used to create a MongoClient instance. The URI describes the hosts to
 * be used and options.
 * <p>The format of the URI is:
 * <pre>
 *   mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database[.collection]][?options]]
 * </pre>
 * <ul>
 * <li>{@code mongodb://} is a required prefix to identify that this is a string in the standard connection format.</li>
 * <li>{@code username:password@} are optional.  If given, the driver will attempt to login to a database after
 * connecting to a database server.</li>
 * <li>{@code host1} is the only required part of the URI.  It identifies a server address to connect to.</li>
 * <li>{@code :portX} is optional and defaults to :27017 if not provided.</li>
 * <li>{@code /database} is the name of the database to login to and thus is only relevant if the
 * {@code username:password@} syntax is used. If not specified the "admin" database will be used by default.</li>
 * <li>{@code ?options} are connection options. Note that if {@code database} is absent there is still a {@code /}
 * required between the last host and the {@code ?} introducing the options. Options are name=value pairs and the pairs
 * are separated by "&amp;". For backwards compatibility, ";" is accepted as a separator in addition to "&amp;",
 * but should be considered as deprecated.</li>
 * </ul>
 * <p>
 * The Java driver supports the following options (case insensitive):
 * <p>
 * Replica set configuration:
 * </p>
 * <ul>
 * <li>{@code replicaSet=name}: Implies that the hosts given are a seed list, and the driver will attempt to find
 * all members of the set.</li>
 * </ul>
 * <p>Connection Configuration:</p>
 * <ul>
 * <li>{@code ssl=true|false}: Whether to connect using SSL.</li>
 * <li>{@code connectTimeoutMS=ms}: How long a connection can take to be opened before timing out.</li>
 * <li>{@code socketTimeoutMS=ms}: How long a send or receive on a socket can take before timing out.</li>
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
 * </ul>
 * <p>Authentication configuration:</p>
 * <ul>
 * <li>{@code authMechanism=MONGO-CR|GSSAPI}: The authentication mechanism to use if a credential was supplied.
 * The default is MONGODB-CR, which is the native MongoDB Challenge Response mechanism.
 * </li>
 * <li>{@code authSource=string}: The source of the authentication credentials.  This is typically the database that
 * the credentials have been created.  The value defaults to the database specified in the path portion of the URI.
 * If the database is specified in neither place, the default value is "admin".  For GSSAPI, it's not necessary to specify
 * a source.
 * </li>
 * <ul>
 * <p>
 * Note: This class is a replacement for {@code MongoURI}, to be used with {@code MongoClient}.  The main difference
 * in behavior is that the default write concern is {@code WriteConcern.ACKNOWLEDGED}.
 * </p>
 *
 * @see MongoClientOptions for the default values for all options
 * @since 2.10.0
 */
public class MongoClientURI {

    private static final String PREFIX = "mongodb://";
    private static final String UTF_8 = "UTF-8";

    /**
     * Creates a MongoURI from the given string.
     *
     * @param uri the URI
     * @dochub connections
     */
    public MongoClientURI(final String uri) {
        this(uri, new MongoClientOptions.Builder());
    }

    /**
     * Creates a MongoURI from the given URI string, and MongoClientOptions.Builder.  The builder can be configured
     * with default options, which may be overridden by options specified in the URI string.
     *
     * @param uri  the URI
     * @param builder a Builder
     * @see com.mongodb.MongoClientURI#getOptions()
     * @since 2.11.0
     */
    public MongoClientURI(String uri, MongoClientOptions.Builder builder) {
        try {
            this.uri = uri;
            if (!uri.startsWith(PREFIX))
                throw new IllegalArgumentException("uri needs to start with " + PREFIX);

            uri = uri.substring(PREFIX.length());

            String serverPart;
            String nsPart;
            String optionsPart;
            String userName = null;
            char[] password = null;

            {
                int idx = uri.lastIndexOf("/");
                if (idx < 0) {
                    if (uri.contains("?")) {
                        throw new IllegalArgumentException("URI contains options without trailing slash");
                    }
                    serverPart = uri;
                    nsPart = null;
                    optionsPart = "";
                } else {
                    serverPart = uri.substring(0, idx);
                    nsPart = uri.substring(idx + 1);

                    idx = nsPart.indexOf("?");
                    if (idx >= 0) {
                        optionsPart = nsPart.substring(idx + 1);
                        nsPart = nsPart.substring(0, idx);
                    } else {
                        optionsPart = "";
                    }

                }
            }

            { // userName,password,hosts
                List<String> all = new LinkedList<String>();

                int idx = serverPart.indexOf("@");

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

                hosts = Collections.unmodifiableList(all);
            }

            if (nsPart != null && nsPart.length() != 0) { // database,_collection
                int idx = nsPart.indexOf(".");
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
            options = createOptions(optionsMap, builder);
            credentials = createCredentials(optionsMap, userName, password, database);
            warnOnUnsupportedOptions(optionsMap);
        } catch (UnsupportedEncodingException e) {
            throw new MongoInternalException("This should not happen", e);
        }
    }

    static Set<String> generalOptionsKeys = new HashSet<String>();
    static Set<String> authKeys = new HashSet<String>();
    static Set<String> readPreferenceKeys = new HashSet<String>();
    static Set<String> writeConcernKeys = new HashSet<String>();
    static Set<String> allKeys = new HashSet<String>();

    static {
        generalOptionsKeys.add("maxpoolsize");
        generalOptionsKeys.add("waitqueuemultiple");
        generalOptionsKeys.add("waitqueuetimeoutms");
        generalOptionsKeys.add("connecttimeoutms");
        generalOptionsKeys.add("sockettimeoutms");
        generalOptionsKeys.add("sockettimeoutms");
        generalOptionsKeys.add("autoconnectretry");
        generalOptionsKeys.add("ssl");

        readPreferenceKeys.add("slaveok");
        readPreferenceKeys.add("readpreference");
        readPreferenceKeys.add("readpreferencetags");

        writeConcernKeys.add("safe");
        writeConcernKeys.add("w");
        writeConcernKeys.add("wtimeout");
        writeConcernKeys.add("fsync");
        writeConcernKeys.add("j");

        authKeys.add("authmechanism");
        authKeys.add("authsource");

        allKeys.addAll(generalOptionsKeys);
        allKeys.addAll(authKeys);
        allKeys.addAll(readPreferenceKeys);
        allKeys.addAll(writeConcernKeys);
    }

    private void warnOnUnsupportedOptions(Map<String, List<String>> optionsMap) {
        for (String key : optionsMap.keySet()) {
            if (!allKeys.contains(key)) {
                LOGGER.warning("Unknown or Unsupported Option '" + key + "'");
            }
        }
    }

    private MongoClientOptions createOptions(Map<String, List<String>> optionsMap, MongoClientOptions.Builder builder) {
        for (String key : generalOptionsKeys) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("maxpoolsize")) {
                builder.connectionsPerHost(Integer.parseInt(value));
            } else if (key.equals("waitqueuemultiple")) {
                builder.threadsAllowedToBlockForConnectionMultiplier(Integer.parseInt(value));
            } else if (key.equals("waitqueuetimeoutms")) {
                builder.maxWaitTime(Integer.parseInt(value));
            } else if (key.equals("connecttimeoutms")) {
                builder.connectTimeout(Integer.parseInt(value));
            } else if (key.equals("sockettimeoutms")) {
                builder.socketTimeout(Integer.parseInt(value));
            } else if (key.equals("autoconnectretry")) {
                builder.autoConnectRetry(_parseBoolean(value));
            } else if (key.equals("ssl")) {
                if (_parseBoolean(value)) {
                    builder.socketFactory(SSLSocketFactory.getDefault());
                }
            }
        }

        WriteConcern writeConcern = createWriteConcern(optionsMap);
        ReadPreference readPreference = createReadPreference(optionsMap);

        if (writeConcern != null) {
            builder.writeConcern(writeConcern);
        }
        if (readPreference != null) {
            builder.readPreference(readPreference);
        }

        return builder.build();
    }

    private WriteConcern createWriteConcern(final Map<String, List<String>> optionsMap) {
        Boolean safe = null;
        String w = null;
        int wTimeout = 0;
        boolean fsync = false;
        boolean journal = false;

        for (String key : writeConcernKeys) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("safe")) {
                safe = _parseBoolean(value);
            } else if (key.equals("w")) {
                w = value;
            } else if (key.equals("wtimeout")) {
                wTimeout = Integer.parseInt(value);
            } else if (key.equals("fsync")) {
                fsync = _parseBoolean(value);
            } else if (key.equals("j")) {
                journal = _parseBoolean(value);
            }
        }
        return buildWriteConcern(safe, w, wTimeout, fsync, journal);
    }

    private ReadPreference createReadPreference(final Map<String, List<String>> optionsMap) {
        Boolean slaveOk = null;
        String readPreferenceType = null;
        DBObject firstTagSet = null;
        List<DBObject> remainingTagSets = new ArrayList<DBObject>();

        for (String key : readPreferenceKeys) {
            String value = getLastValue(optionsMap, key);
            if (value == null) {
                continue;
            }

            if (key.equals("slaveok")) {
                slaveOk = _parseBoolean(value);
            } else if (key.equals("readpreference")) {
                readPreferenceType = value;
            } else if (key.equals("readpreferencetags")) {
                for (String cur : optionsMap.get(key)) {
                    DBObject tagSet = getTagSet(cur.trim());
                    if (firstTagSet == null) {
                        firstTagSet = tagSet;
                    } else {
                        remainingTagSets.add(tagSet);
                    }
                }
            }
        }
        return buildReadPreference(readPreferenceType, firstTagSet, remainingTagSets, slaveOk);
    }

    private MongoCredential createCredentials(Map<String, List<String>> optionsMap, final String userName,
                                               final char[] password, String database) {
        if (userName == null) {
            return null;
        }

        if (database == null) {
            database = "admin";
        }

        String mechanism = MongoCredential.MONGODB_CR_MECHANISM;
        String authSource = database;

        for (String key : authKeys) {
            String value = getLastValue(optionsMap, key);

            if (value == null) {
                continue;
            }

            if (key.equals("authmechanism")) {
                mechanism = value;
            } else if (key.equals("authsource")) {
                authSource = value;
            }
        }

        if (mechanism.equals(MongoCredential.GSSAPI_MECHANISM)) {
            return MongoCredential.createGSSAPICredential(userName);
        }
        else if (mechanism.equals(MongoCredential.MONGODB_CR_MECHANISM)) {
            return MongoCredential.createMongoCRCredential(userName, authSource, password);
        }
        else {
             throw new IllegalArgumentException("Unsupported authMechanism: " + mechanism);
        }
    }

    private String getLastValue(final Map<String, List<String>> optionsMap, final String key) {
        List<String> valueList = optionsMap.get(key);
        if (valueList == null) {
            return null;
        }
        return valueList.get(valueList.size() - 1);
    }

    private Map<String, List<String>> parseOptions(String optionsPart) {
        Map<String, List<String>> optionsMap = new HashMap<String, List<String>>();

        for (String _part : optionsPart.split("&|;")) {
            int idx = _part.indexOf("=");
            if (idx >= 0) {
                String key = _part.substring(0, idx).toLowerCase();
                String value = _part.substring(idx + 1);
                List<String> valueList = optionsMap.get(key);
                if (valueList == null) {
                    valueList = new ArrayList<String>(1);
                }
                valueList.add(value);
                optionsMap.put(key, valueList);
            }
        }

        return optionsMap;
    }

    private ReadPreference buildReadPreference(final String readPreferenceType, final DBObject firstTagSet,
                                               final List<DBObject> remainingTagSets, final Boolean slaveOk) {
        if (readPreferenceType != null) {
            if (firstTagSet == null) {
                return ReadPreference.valueOf(readPreferenceType);
            } else {
                return ReadPreference.valueOf(readPreferenceType, firstTagSet,
                        remainingTagSets.toArray(new DBObject[remainingTagSets.size()]));
            }
        } else if (slaveOk != null) {
            if (slaveOk.equals(Boolean.TRUE)) {
                return ReadPreference.secondaryPreferred();
            }
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

    private DBObject getTagSet(String tagSetString) {
        DBObject tagSet = new BasicDBObject();
        if (tagSetString.length() > 0) {
            for (String tag : tagSetString.split(",")) {
                String[] tagKeyValuePair = tag.split(":");
                if (tagKeyValuePair.length != 2) {
                    throw new IllegalArgumentException("Bad read preference tags: " + tagSetString);
                }
                tagSet.put(tagKeyValuePair[0].trim(), tagKeyValuePair[1].trim());
            }
        }
        return tagSet;
    }

    boolean _parseBoolean(String _in) {
        String in = _in.trim();
        return in != null && in.length() > 0 && (in.equals("1") || in.toLowerCase().equals("true") || in.toLowerCase()
                .equals("yes"));
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
    public MongoCredential getCredentials() {
        return credentials;
    }

    /**
     * Gets the options
     *
     * @return the MongoClientOptions based on this URI.
     */
    public MongoClientOptions getOptions() {
        return options;
    }

    // ---------------------------------

    private final MongoClientOptions options;
    private final MongoCredential credentials;
    private final List<String> hosts;
    private final String database;
    private final String collection;
    private final String uri;


    static final Logger LOGGER = Logger.getLogger("com.mongodb.MongoURI");

    @Override
    public String toString() {
        return uri;
    }
}
