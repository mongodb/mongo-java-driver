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

import com.mongodb.annotations.Immutable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * <p>Controls the acknowledgment of write operations with various options.</p>
 *
 * <p>{@code w}</p>
 * <ul>
 *  <li> 0: Don't wait for acknowledgement from the server </li>
 *  <li> 1: Wait for acknowledgement, but don't wait for secondaries to replicate</li>
 *  <li> &gt;=2: Wait for one or more secondaries to also acknowledge </li>
 * </ul>
 * <p>{@code wtimeout} - how long to wait for slaves before failing</p>
 * <ul>
 *   <li>0: indefinite </li>
 *   <li>&gt;0: time to wait in milliseconds</li>
 * </ul>
 *
 * <p>Other options:</p>
 * <ul>
 *   <li>{@code j}: If true block until write operations have been committed to the journal. Cannot be used in combination with
 *   {@code fsync}. Prior to MongoDB 2.6 this option was ignored if the server was running without journaling.  Starting with MongoDB 2.6
 *   write operations will fail with an exception if this option is used when the server is running without journaling.</li>
 *   <li>{@code fsync}: If true and the server is running without journaling, blocks until the server has synced all data files to disk.
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

    private final Object w;

    private final int wtimeout;

    private final boolean fsync;

    private final boolean j;

    /**
     * Write operations that use this write concern will wait for acknowledgement from the primary server before returning. Exceptions are
     * raised for network issues, and server errors.
     *
     * @since 2.10.0
     */
    public static final WriteConcern ACKNOWLEDGED = new WriteConcern(1);

    /**
     * Write operations that use this write concern will return as soon as the message is written to the socket. Exceptions are raised for
     * network issues, but not server errors.
     *
     * @since 2.10.0
     */
    public static final WriteConcern UNACKNOWLEDGED = new WriteConcern(0);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush the data to disk.
     */
    public static final WriteConcern FSYNCED = new WriteConcern(true);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to group commit to the journal
     * file on disk.
     */
    public static final WriteConcern JOURNALED = new WriteConcern(1, 0, false, true);

    /**
     * Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.
     */
    public static final WriteConcern REPLICA_ACKNOWLEDGED = new WriteConcern(2);

    /**
     * <p>Write operations that use this write concern will return as soon as the message is written to the socket. Exceptions are raised
     * for network issues, but not server errors.</p>
     *
     * <p>This field has been superseded by {@code WriteConcern.UNACKNOWLEDGED}, and may be deprecated in a future release.</p>
     *
     * @see WriteConcern#UNACKNOWLEDGED
     */
    public static final WriteConcern NORMAL = UNACKNOWLEDGED;

    /**
     * <p>Write operations that use this write concern will wait for acknowledgement from the primary server before returning. Exceptions
     * are raised for network issues, and server errors.</p>
     *
     * <p>This field has been superseded by {@code WriteConcern.ACKNOWLEDGED}, and may be deprecated in a future release.</p>
     *
     * @see WriteConcern#ACKNOWLEDGED
     */
    public static final WriteConcern SAFE = ACKNOWLEDGED;

    /**
     * Exceptions are raised for network issues, and server errors; waits on a majority of servers for the write operation.
     */
    public static final WriteConcern MAJORITY = new WriteConcern("majority");

    /**
     * <p>Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush the data to
     * disk.</p>
     *
     * <p>This field has been superseded by {@code WriteConcern.FSYNCED}, and may be deprecated in a future release.</p>
     *
     * @see WriteConcern#FSYNCED
     */
    public static final WriteConcern FSYNC_SAFE = FSYNCED;

    /**
     * <p>Exceptions are raised for network issues, and server errors; the write operation waits for the server to group commit to the
     * journal file on disk. </p>
     *
     * <p>This field has been superseded by {@code WriteConcern.JOURNALED}, and may be deprecated in a future release.</p>
     *
     * @see WriteConcern#JOURNALED
     */
    public static final WriteConcern JOURNAL_SAFE = JOURNALED;

    /**
     * <p>Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.</p>
     *
     * <p>This field has been superseded by {@code WriteConcern.REPLICA_ACKNOWLEDGED}, and may be deprecated in a future release.</p>
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
     * <p>Creates a WriteConcern object.</p>
     *
     * <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior </p>
     *
     * <p> {@code w} represents the number of servers:</p>
     * <ul>
     *     <li>{@code w=-1} None, no checking is done</li>
     *     <li>{@code w=0} None, network socket errors raised</li>
     *     <li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     *     <li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * </ul>
     *
     * @param w        number of writes
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     */
    public WriteConcern(final int w, final int wtimeout, final boolean fsync) {
        this(w, wtimeout, fsync, false);
    }

    /**
     * <p>Creates a WriteConcern object.</p>
     *
     * <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior</p>
     *
     * <p> {@code w} represents the number of servers:</p>
     * <ul>
     *     <li>{@code w=-1} None, no checking is done</li>
     *     <li>{@code w=0} None, network socket errors raised</li>
     *     <li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     *     <li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * </ul>
     *
     * @param w        number of writes
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     * @param j        whether writes should wait for a journaling group commit
     */
    public WriteConcern(final int w, final int wtimeout, final boolean fsync, final boolean j) {
        isTrueArgument("w >= 0", w >= 0);
        this.w = w;
        this.wtimeout = wtimeout;
        this.fsync = fsync;
        this.j = j;
    }

    /**
     * <p>Creates a WriteConcern object.</p>
     *
     * <p>Specifies the number of servers to wait for on the write operation, and exception raising behavior</p>
     *
     * <p> {@code w} represents the number of servers:</p>
     * <ul>
     *     <li>{@code w=-1} None, no checking is done</li>
     *     <li>{@code w=0} None, network socket errors raised</li>
     *     <li>{@code w=1} Checks server for errors as well as network socket errors raised</li>
     *     <li>{@code w>1} Checks servers (w) for errors as well as network socket errors raised</li>
     * </ul>
     *
     * @param w        number of writes
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     * @param j        whether writes should wait for a journaling group commit
     */
    public WriteConcern(final String w, final int wtimeout, final boolean fsync, final boolean j) {
        this.w = notNull("w", w);
        this.wtimeout = wtimeout;
        this.fsync = fsync;
        this.j = j;
    }

    /**
     * Gets the w value (the write strategy)
     *
     * @return w, either an instance of Integer or String
     */
    public Object getWObject() {
        return w;
    }

    /**
     * Gets the w parameter (the write strategy)
     *
     * @return w, as an int
     * @throws ClassCastException if w is not an integer
     */
    public int getW() {
        return (Integer) w;
    }

    /**
     * Gets the w parameter (the write strategy) in String format
     *
     * @return w as a string
     * @throws ClassCastException if w is not a String
     */
    public String getWString() {
        return (String) w;
    }

    /**
     * Gets the write timeout (in milliseconds)
     *
     * @return the timeout
     */
    public int getWtimeout() {
        return wtimeout;
    }

    /**
     * Gets the fsync flag (fsync to disk on the server)
     *
     * @return the fsync flag
     */
    public boolean getFsync() {
        return fsync();
    }

    /**
     * Gets the fsync flag (fsync to disk on the server)
     *
     * @return the fsync flag
     */
    public boolean fsync() {
        return fsync;
    }

    /**
     * Returns whether "getlasterror" should be called (w &gt; 0)
     *
     * @return whether this write concern will result in an an acknowledged write
     */
    public boolean callGetLastError() {
        return isAcknowledged();
    }

    /**
     * The server default is w == 1 and everything else the default value.
     *
     * @return true if this write concern is the server's default
     * @mongodb.driver.manual /reference/replica-configuration/#local.system.replset.settings.getLastErrorDefaults getLastErrorDefaults
     */
    public boolean isServerDefault() {
        return w.equals(1) && wtimeout == 0 && !fsync && !j;
    }

    /**
     * Gets this write concern as a document
     *
     * @return The write concern as a Document, even if {@code w &lt;= 0}
     */
    public BsonDocument asDocument() {
        if (!isAcknowledged()) {
            throw new IllegalStateException("The write is unacknowledged, so no document can be created");
        }
        BsonDocument document = new BsonDocument();

        addW(document);

        addWTimeout(document);
        addFSync(document);
        addJ(document);

        return document;
    }

    /**
     * Returns whether write operations should be acknowledged
     *
     * @return true w != null or w &gt; 0
     */
    public boolean isAcknowledged() {
        if (w instanceof Integer) {
            return (Integer) w > 0;
        }
        return w != null;
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

        if (fsync != that.fsync) {
            return false;
        }
        if (j != that.j) {
            return false;
        }
        if (wtimeout != that.wtimeout) {
            return false;
        }
        return w.equals(that.w);
    }

    @Override
    public int hashCode() {
        int result = w.hashCode();
        result = 31 * result + wtimeout;
        result = 31 * result + (fsync ? 1 : 0);
        result = 31 * result + (j ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WriteConcern{w=" + w + ", wtimeout=" + wtimeout + ", fsync=" + fsync + ", j=" + j;

    }

    /**
     * Gets the j parameter (journal syncing)
     *
     * @return true if j is set
     */
    public boolean getJ() {
        return j;
    }

    /**
     * @param w an int representation of the write concern
     * @return the WriteConcern matching the given w value
     */
    public WriteConcern withW(final int w) {
        return new WriteConcern(w, getWtimeout(), getFsync(), getJ());
    }

    /**
     * @param w a String representation of the write concern
     * @return the WriteConcern
     */
    public WriteConcern withW(final String w) {
        return new WriteConcern(w, getWtimeout(), getFsync(), getJ());
    }

    /**
     * @param fsync true if the write concern needs to include fsync
     * @return the WriteConcern
     */
    public WriteConcern withFsync(final boolean fsync) {
        if (getWObject() instanceof Integer) {
            return new WriteConcern(getW(), getWtimeout(), fsync, getJ());
        } else {
            return new WriteConcern(getWString(), getWtimeout(), fsync, getJ());
        }
    }

    /**
     * @param j true if journalling is required
     * @return the WriteConcern
     */
    public WriteConcern withJ(final boolean j) {
        if (getWObject() instanceof Integer) {
            return new WriteConcern(getW(), getWtimeout(), getFsync(), j);
        } else {
            return new WriteConcern(getWString(), getWtimeout(), getFsync(), j);
        }
    }

    private void addW(final BsonDocument document) {
        if (w instanceof String) {
            document.put("w", new BsonString((String) w));
        } else {
            document.put("w", new BsonInt32((Integer) w));
        }
    }

    private void addJ(final BsonDocument document) {
        if (j) {
            document.put("j", BsonBoolean.TRUE);
        }
    }

    private void addFSync(final BsonDocument document) {
        if (fsync) {
            document.put("fsync", BsonBoolean.TRUE);
        }
    }

    private void addWTimeout(final BsonDocument document) {
        if (wtimeout > 0) {
            document.put("wtimeout", new BsonInt32(wtimeout));
        }
    }


    /**
     * Create a Majority Write Concern that requires a majority of servers to acknowledge the write.
     *
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     * @param j        whether writes should wait for a journal group commit
     * @return Majority, a subclass of WriteConcern that represents the write concern requiring most servers to acknowledge the write
     */
    public static Majority majorityWriteConcern(final int wtimeout, final boolean fsync, final boolean j) {
        return new Majority(wtimeout, fsync, j);
    }

    /**
     * A write concern that blocks acknowledgement of a write operation until a majority of replica set members have applied it.
     */
    public static class Majority extends WriteConcern {

        private static final long serialVersionUID = -4128295115883875212L;

        /**
         * Create a new Majority WriteConcern.
         */
        public Majority() {
            this(0, false, false);
        }

        /**
         * Create a new WriteConcern with the given configuration.
         *
         * @param wtimeout timeout for write operation
         * @param fsync    whether or not to fsync
         * @param j        whether writes should wait for a journaling group commit
         */
        public Majority(final int wtimeout, final boolean fsync, final boolean j) {
            super("majority", wtimeout, fsync, j);
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
