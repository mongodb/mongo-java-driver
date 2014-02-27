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

// WriteConcern.java

package com.mongodb;

import org.bson.BSONObject;
import org.mongodb.annotations.Immutable;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>Controls the acknowledgment of write operations with various options.
 * <p>
 * <b>w</b>
 * <ul>
 *  <li> 0 = Don't wait for acknowledgement from the server </li>
 *  <li> 1 = Wait for acknowledgement, but don't wait for secondaries to replicate</li>
 *  <li> 2+= Wait for one or more secondaries to also acknowledge </li>
 * </ul>
 * <b>wtimeout</b> how long to wait for slaves before failing
 * <ul>
 *   <li>0: indefinite </li>
 *   <li>greater than 0: ms to wait </li>
 * </ul>
 * </p>
 * <p>
 * Other options:
 * <ul>
 *   <li><b>j</b>: If true block until write operations have been committed to the journal. Cannot be used in combination with
 *   {@code fsync}. Prior to MongoDB 2.6 this option was ignored if the server was running without journaling.  Starting with MongoDB 2.6
 *   write operations will fail with an exception if this option is used when the server is running without journaling.</li>
 *   <li><b>fsync</b>: If true and the server is running without journaling, blocks until the server has synced all data files to disk.
 *   If the server is running with journaling, this acts the same as the {@code j} option, blocking until write operations have been
 *   committed to the journal. Cannot be used in combination with {@code j}. In almost all cases the  {@code j} flag should be used in
 *   preference to this one.</li>
 * </ul>
 *
 * @mongodb.driver.manual core/write-concern/ Write Concern
 */
@Immutable
public class WriteConcern implements Serializable {

    private static final long serialVersionUID = 1884671104750417011L;

    // map of the constants from above for use by fromString
    private static final Map<String, WriteConcern> NAMED_CONCERNS;

    private final org.mongodb.WriteConcern proxied;
    private final boolean continueOnError;

    /**
     * Write operations that use this write concern will wait for acknowledgement from the primary server before returning. Exceptions are
     * raised for network issues, and server errors.
     *
     * @since 2.10.0
     */
    public static final WriteConcern ACKNOWLEDGED = new WriteConcern(org.mongodb.WriteConcern.ACKNOWLEDGED);
    /**
     * Write operations that use this write concern will return as soon as the message is written to the socket. Exceptions are raised for
     * network issues, but not server errors.
     *
     * @since 2.10.0
     */
    public static final WriteConcern UNACKNOWLEDGED = new WriteConcern(org.mongodb.WriteConcern.UNACKNOWLEDGED);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush the data to disk.
     */
    public static final WriteConcern FSYNCED = new WriteConcern(org.mongodb.WriteConcern.FSYNCED);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to group commit to the journal
     * file on disk.
     */
    public static final WriteConcern JOURNALED = new WriteConcern(org.mongodb.WriteConcern.JOURNALED);

    /**
     * Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.
     */
    public static final WriteConcern REPLICA_ACKNOWLEDGED = new WriteConcern(org.mongodb.WriteConcern.REPLICA_ACKNOWLEDGED);

    /**
     * Write operations that use this write concern will return as soon as the message is written to the socket. Exceptions are raised for
     * network issues, but not server errors.
     * <p/>
     * This field has been superseded by {@code WriteConcern.UNACKNOWLEDGED}, and may be deprecated in a future release.
     *
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public static final WriteConcern NORMAL = UNACKNOWLEDGED;

    /**
     * Write operations that use this write concern will wait for acknowledgement from the primary server before returning. Exceptions are
     * raised for network issues, and server errors.
     * <p/>
     * This field has been superseded by {@code WriteConcern.ACKNOWLEDGED}, and may be deprecated in a future release.
     *
     * @see WriteConcern#ACKNOWLEDGED
     */
    public static final WriteConcern SAFE = ACKNOWLEDGED;

    /**
     * Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation.
     */
    public static final WriteConcern MAJORITY = new WriteConcern("majority");

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush the data to disk.
     * <p/>
     * This field has been superseded by {@code WriteConcern.FSYNCED}, and may be deprecated in a future release.
     *
     * @see WriteConcern#FSYNCED
     */
    public static final WriteConcern FSYNC_SAFE = FSYNCED;

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to group commit to the journal
     * file on disk.
     * <p/>
     * This field has been superseded by {@code WriteConcern.JOURNALED}, and may be deprecated in a future release.
     *
     * @see WriteConcern#JOURNALED
     */
    public static final WriteConcern JOURNAL_SAFE = JOURNALED;

    /**
     * Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.
     * <p/>
     * This field has been superseded by {@code WriteConcern.REPLICA_ACKNOWLEDGED}, and may be deprecated in a future release.
     *
     * @see WriteConcern#REPLICA_ACKNOWLEDGED
     */
    public static final WriteConcern REPLICAS_SAFE = REPLICA_ACKNOWLEDGED;

    /**
     * Default constructor keeping all options as default.  Be careful using this constructor, as it's equivalent to {@code
     * WriteConcern.UNACKNOWLEDGED}, so writes may be lost without any errors being reported.
     *
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public WriteConcern() {
        this(0);
    }

    /**
     * Calls {@link WriteConcern#WriteConcern(int, int, boolean)} with wtimeout=0 and fsync=false
     *
     * @param w number of writes
     */
    public WriteConcern(final int w) {
        this(w, 0, false);
    }

    /**
     * Tag based Write Concern with wtimeout=0, fsync=false, and j=false
     *
     * @param w Write Concern tag
     */
    public WriteConcern(final String w) {
        this(w, 0, false, false);
    }

    /**
     * Calls {@link WriteConcern#WriteConcern(int, int, boolean)} with fsync=false
     *
     * @param w        number of writes
     * @param wtimeout timeout for write operation
     */
    public WriteConcern(final int w, final int wtimeout) {
        this(w, wtimeout, false);
    }

    /**
     * Calls {@link WriteConcern#WriteConcern(int, int, boolean)} with w=1 and wtimeout=0
     *
     * @param fsync whether or not to fsync
     */
    public WriteConcern(final boolean fsync) {
        this(1, 0, fsync);
    }

    /**
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior
     * </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking is done</li> <li>{@code w=0} None, network
     * socket errors raised</li> <li>{@code w=1} Checks server for errors as well as network socket errors raised</li> <li>{@code w>1}
     * Checks servers (w) for errors as well as network socket errors raised</li> </ul> </p>
     *
     * @param w        number of writes
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     */
    public WriteConcern(final int w, final int wtimeout, final boolean fsync) {
        this(w, wtimeout, fsync, false);
    }

    /**
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior
     * </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking is done</li> <li>{@code w=0} None, network
     * socket errors raised</li> <li>{@code w=1} Checks server for errors as well as network socket errors raised</li> <li>{@code w>1}
     * Checks servers (w) for errors as well as network socket errors raised</li> </ul> </p>
     *
     * @param w        number of writes
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     * @param j        whether writes should wait for a journaling group commit
     */
    public WriteConcern(final int w, final int wtimeout, final boolean fsync, final boolean j) {
        this(w, wtimeout, fsync, j, false);
    }

    /**
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior
     * </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking is done</li> <li>{@code w=0} None, network
     * socket errors raised</li> <li>{@code w=1} Checks server for errors as well as network socket errors raised</li> <li>{@code w>1}
     * Checks servers (w) for errors as well as network socket errors raised</li> </ul> </p>
     *
     * @param w               number of writes
     * @param wtimeout        timeout for write operation
     * @param fsync           whether or not to fsync
     * @param j               whether writes should wait for a journaling group commit
     * @param continueOnError if batch writes should continue after the first error
     */
    public WriteConcern(final int w, final int wtimeout, final boolean fsync, final boolean j, final boolean continueOnError) {
        proxied = new org.mongodb.WriteConcern(w, wtimeout, fsync, j);
        this.continueOnError = continueOnError;
    }

    /**
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior
     * </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking is done</li> <li>{@code w=0} None, network
     * socket errors raised</li> <li>{@code w=1} Checks server for errors as well as network socket errors raised</li> <li>{@code w>1}
     * Checks servers (w) for errors as well as network socket errors raised</li> </ul> </p>
     *
     * @param w        number of writes
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     * @param j        whether writes should wait for a journaling group commit
     */
    public WriteConcern(final String w, final int wtimeout, final boolean fsync, final boolean j) {
        this(w, wtimeout, fsync, j, false);
    }

    /**
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior
     * </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking is done</li> <li>{@code w=0} None, network
     * socket errors raised</li> <li>{@code w=1} Checks server for errors as well as network socket errors raised</li> <li>{@code w>1}
     * Checks servers (w) for errors as well as network socket errors raised</li> </ul> </p>
     *
     * @param w               number of writes
     * @param wtimeout        timeout for write operation
     * @param fsync           whether or not to fsync
     * @param j               whether writes should wait for a journaling group commit
     * @param continueOnError if batch writes should continue after the first error
     */
    public WriteConcern(final String w, final int wtimeout, final boolean fsync, final boolean j, final boolean continueOnError) {
        proxied = new org.mongodb.WriteConcern(w, wtimeout, fsync, j);
        this.continueOnError = continueOnError;
    }

    /**
     * Creates a WriteConcern based on an instance of org.mongodb.WriteConcern.
     *
     * @param proxied the write concern to copy
     */
    WriteConcern(final org.mongodb.WriteConcern proxied) {
        this(proxied, false);
    }

    /**
     * Creates a WriteConcern based on an instance of org.mongodb.WriteConcern.
     *
     * @param proxied the write concern to copy
     * @param continueOnError if batch writes should continue after the first error
     *
     */
    WriteConcern(final org.mongodb.WriteConcern proxied, final boolean continueOnError) {
        this.proxied = proxied;
        this.continueOnError = continueOnError;
    }

    /**
     * Gets the getlasterror command for this write concern.
     *
     * @return getlasterror command, even if {@code w <= 0}
     */
    public BasicDBObject getCommand() {
        BasicDBObject command = new BasicDBObject("getlasterror", 1);
        command.putAll((BSONObject) DBObjects.toDBObject(proxied.asDocument()));
        return command;
    }

    /**
     * Gets the w value (the write strategy)
     *
     * @return w, either an instance of Integer or String
     */
    public Object getWObject() {
        return proxied.getWObject();
    }

    /**
     * Gets the w parameter (the write strategy)
     *
     * @return w, as an int
     * @throws ClassCastException if w is not an integer
     */
    public int getW() {
        return proxied.getW();
    }

    /**
     * Gets the w parameter (the write strategy) in String format
     *
     * @return w as a string
     * @throws ClassCastException if w is not a String
     */
    public String getWString() {
        return proxied.getWString();
    }

    /**
     * Gets the write timeout (in milliseconds)
     *
     * @return the timeout
     */
    public int getWtimeout() {
        return proxied.getWtimeout();
    }

    /**
     * Gets the fsync flag (fsync to disk on the server)
     *
     * @return the fsync flag
     */
    public boolean getFsync() {
        return proxied.getFsync();
    }

    /**
     * Gets the fsync flag (fsync to disk on the server)
     *
     * @return the fsync flag
     */
    public boolean fsync() {
        return proxied.getFsync();
    }

    /**
     * Returns whether "getlasterror" should be called (w > 0)
     *
     * @return whether this write concern will result in an an acknowledged write
     */
    public boolean callGetLastError() {
        return proxied.isAcknowledged();
    }

    /**
     * Gets the WriteConcern constants by name (matching is done case insensitively).
     *
     * @param name the name of the WriteConcern
     * @return the {@code WriteConcern instance}
     */
    public static WriteConcern valueOf(final String name) {
        return NAMED_CONCERNS.get(name.toLowerCase());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WriteConcern that = (WriteConcern) o;

        if (!proxied.equals(that.proxied)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return proxied.hashCode();
    }

    @Override
    public String toString() {
        return proxied.toString();
    }

    /**
     * Gets the j parameter (journal syncing)
     *
     * @return true if j is set
     */
    public boolean getJ() {
        return proxied.getJ();
    }

    /**
     * Gets the "continue on error" mode
     *
     * @return true if set to continue on error
     */
    public boolean getContinueOnError() {
        return continueOnError;
    }

    public org.mongodb.WriteConcern toNew() {
        return proxied;
    }

    /**
     * Toggles the "continue inserts on error" mode. This only applies to server side errors. If there is a document which does not validate
     * in the client, an exception will still be thrown in the client. This will return a new WriteConcern instance with the specified
     * continueOnInsert value.
     *
     * @param continueOnError true if the operation should continue when the server continues an error
     */
    public WriteConcern continueOnError(final boolean continueOnError) {
        return new WriteConcern(proxied, continueOnError);
    }

    /**
     * Create a Majority Write Concern that requires a majority of servers to acknowledge the write.
     *
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     * @param j        whether writes should wait for a journal group commit
     */
    public static Majority majorityWriteConcern(final int wtimeout, final boolean fsync, final boolean j) {
        return new Majority(wtimeout, fsync, j);
    }

    public static class Majority extends WriteConcern {

        private static final long serialVersionUID = -4128295115883875212L;

        public Majority() {
            this(0, false, false);
        }

        public Majority(final int wtimeout, final boolean fsync, final boolean j) {
            super("majority", wtimeout, fsync, j);
        }

        @Override
        public String toString() {
            return "WriteConcern.Majority{"
                   + "command=" + getCommand()
                   + "}";
        }
    }

    static {
        NAMED_CONCERNS = new HashMap<String, WriteConcern>();
        for (final Field f : WriteConcern.class.getFields()) {
            if (Modifier.isStatic(f.getModifiers()) && f.getType().equals(WriteConcern.class)) {
                String key = f.getName().toLowerCase();
                try {
                    NAMED_CONCERNS.put(key, (WriteConcern) f.get(null));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
