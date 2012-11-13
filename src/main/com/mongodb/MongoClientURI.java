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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;


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
 *
 * Note: This class is a replacement for {@code MongoURI}, to be used with {@code MongoClient}.  The main difference
 * in behavior is that the default write concern is {@code WriteConcern.ACKNOWLEDGED}.
 *
 * @see MongoClientOptions for the default values for all options
 * @since 2.10.0
 */
public class MongoClientURI {

    private static final String PREFIX = "mongodb://";

    /**
     * Creates a MongoURI from the given string.
     *
     * @param uri the URI
     * @dochub connections
     */
    public MongoClientURI(final String uri) {
        this(uri, new MongoClientOptions.Builder());
    }

    MongoClientURI(String uri, MongoClientOptions.Builder builder) {
        _uri = uri;
        if (!uri.startsWith(PREFIX))
            throw new IllegalArgumentException("uri needs to start with " + PREFIX);

        uri = uri.substring(PREFIX.length());

        String serverPart;
        String nsPart;
        String optionsPart;

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

        { // userName,password,_hosts
            List<String> all = new LinkedList<String>();

            int idx = serverPart.indexOf("@");

            if (idx > 0) {
                String authPart = serverPart.substring(0, idx);
                serverPart = serverPart.substring(idx + 1);

                idx = authPart.indexOf(":");
                userName = authPart.substring(0, idx);
                password = authPart.substring(idx + 1).toCharArray();
            } else {
                userName = null;
                password = null;
            }

            Collections.addAll(all, serverPart.split(","));

            _hosts = Collections.unmodifiableList(all);
        }

        if (nsPart != null) { // database,_collection
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

        parseOptions(optionsPart, builder);
    }


    private void parseOptions(String optionsPart, MongoClientOptions.Builder builder) {
        String readPreferenceType = null;
        DBObject firstTagSet = null;
        List<DBObject> remainingTagSets = new ArrayList<DBObject>();

        Boolean safe = null;
        String w = null;
        int wTimeout = 0;
        boolean fsync = false;
        boolean journal = false;
        Boolean slaveOk = null;
        for (String _part : optionsPart.split("&|;")) {
            int idx = _part.indexOf("=");
            if (idx >= 0) {
                String key = _part.substring(0, idx).toLowerCase();
                String value = _part.substring(idx + 1);

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
                } else if (key.equals("slaveok")) {
                    slaveOk = _parseBoolean(value);
                } else if (key.equals("readpreference")) {
                    readPreferenceType = value;
                } else if (key.equals("readpreferencetags")) {
                    DBObject tagSet = getTagSet(value.trim());
                    if (firstTagSet == null) {
                        firstTagSet = tagSet;
                    } else {
                        remainingTagSets.add(tagSet);
                    }
                } else if (key.equals("safe")) {
                    safe = _parseBoolean(value);
                } else if (key.equals("w")) {
                    w = value;
                } else if (key.equals("wtimeout")) {
                    wTimeout = Integer.parseInt(value);
                } else if (key.equals("fsync")) {
                    fsync = _parseBoolean(value);
                } else if (key.equals("j")) {
                    journal = _parseBoolean(value);
                } else {
                    LOGGER.warning("Unknown or Unsupported Option '" + key + "'");
                }
            }
        }

        buildWriteConcern(builder, safe, w, wTimeout, fsync, journal);

        buildReadPreference(builder, readPreferenceType, firstTagSet, remainingTagSets, slaveOk);

        _options = builder.build();
    }

    private void buildReadPreference(final MongoClientOptions.Builder builder, final String readPreferenceType,
                                     final DBObject firstTagSet, final List<DBObject> remainingTagSets, final Boolean slaveOk) {
        if (readPreferenceType != null) {
            if (firstTagSet == null) {
                builder.readPreference(ReadPreference.valueOf(readPreferenceType));
            } else {
                builder.readPreference(ReadPreference.valueOf(readPreferenceType, firstTagSet,
                        remainingTagSets.toArray(new DBObject[remainingTagSets.size()])));
            }
        } else if (slaveOk != null) {
            if (slaveOk.equals(Boolean.TRUE)) {
                builder.readPreference(ReadPreference.secondaryPreferred());
            }
        }
    }

    private void buildWriteConcern(final MongoClientOptions.Builder builder, final Boolean safe, final String w,
                                   final int wTimeout, final boolean fsync, final boolean journal) {
        if (w != null || wTimeout != 0 || fsync || journal) {
            if (w == null) {
                builder.writeConcern(new WriteConcern(1, wTimeout, fsync, journal));
            } else {
                try {
                    builder.writeConcern(new WriteConcern(Integer.parseInt(w), wTimeout, fsync, journal));
                } catch (NumberFormatException e) {
                    builder.writeConcern(new WriteConcern(w, wTimeout, fsync, journal));
                }
            }
        } else if (safe != null) {
            if (safe) {
               builder.writeConcern(WriteConcern.ACKNOWLEDGED);
            } else {
                builder.writeConcern(WriteConcern.UNACKNOWLEDGED);
            }
        }
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
        return userName;
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    public char[] getPassword() {
        return password;
    }

    /**
     * Gets the list of hosts
     *
     * @return the host list
     */
    public List<String> getHosts() {
        return _hosts;
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
        return _uri;
    }

    /**
     * Gets the options
     *
     * @return the MongoClientOptions based on this URI.
     */
    public MongoClientOptions getOptions() {
        return _options;
    }

    // ---------------------------------

    final String userName;
    final char[] password;
    final List<String> _hosts;
    final String database;
    final String collection;

    final String _uri;

    private MongoClientOptions _options;

    static final Logger LOGGER = Logger.getLogger("com.mongodb.MongoURI");

    @Override
    public String toString() {
        return _uri;
    }
}
