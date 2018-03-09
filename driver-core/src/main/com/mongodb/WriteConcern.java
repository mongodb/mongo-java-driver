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

// WriteConcern.java

package com.mongodb;

import com.mongodb.annotations.Immutable;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
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
 *  <li> "majority": Wait for a majority of data bearing nodes to acknowledge </li>
 *  <li> "&lt;tag set name&gt;": Wait for one or more secondaries to also acknowledge based on a tag set name</li>
 * </ul>
 * <p>{@code wtimeout} - how long to wait for secondaries to acknowledge before failing</p>
 * <ul>
 *   <li>0: indefinite </li>
 *   <li>&gt;0: time to wait in milliseconds</li>
 * </ul>
 *
 * <p>Other options:</p>
 * <ul>
 *   <li>{@code journal}: If true block until write operations have been committed to the journal. Cannot be used in combination with
 *   {@code fsync}. Prior to MongoDB 2.6 this option was ignored if the server was running without journaling.  Starting with MongoDB 2.6
 *   write operations will fail with an exception if this option is used when the server is running without journaling.</li>
 * </ul>
 *
 * @mongodb.driver.manual core/write-concern Write Concern
 * @mongodb.driver.manual reference/write-concern/ Write Concern Reference
 */
@Immutable
public class WriteConcern implements Serializable {

    private static final long serialVersionUID = 1884671104750417011L;

    // map of the constants from above for use by fromString
    private static final Map<String, WriteConcern> NAMED_CONCERNS;

    private final Object w;

    private final Integer wTimeoutMS;

    private final Boolean fsync;

    private final Boolean journal;

    /**
     * Write operations that use this write concern will wait for acknowledgement, using the default write concern configured on the server.
     *
     * @since 2.10.0
     * @mongodb.driver.manual core/write-concern/#write-concern-acknowledged Acknowledged
     */
    public static final WriteConcern ACKNOWLEDGED = new WriteConcern((Object) null, null, null, null);

    /**
     * Write operations that use this write concern will wait for acknowledgement from a single member.
     *
     * @since 3.2
     * @mongodb.driver.manual reference/write-concern/#w-option w option
     */
    public static final WriteConcern W1 = new WriteConcern(1);

    /**
     * Write operations that use this write concern will wait for acknowledgement from two members.
     *
     * @since 3.2
     * @mongodb.driver.manual reference/write-concern/#w-option w option
     */
    public static final WriteConcern W2 = new WriteConcern(2);

    /**
     * Write operations that use this write concern will wait for acknowledgement from three members.
     *
     * @since 3.2
     * @mongodb.driver.manual reference/write-concern/#w-option w option
     */
    public static final WriteConcern W3 = new WriteConcern(3);


    /**
     * Write operations that use this write concern will return as soon as the message is written to the socket. Exceptions are raised for
     * network issues, but not server errors.
     *
     * @since 2.10.0
     * @mongodb.driver.manual core/write-concern/#unacknowledged Unacknowledged
     */
    public static final WriteConcern UNACKNOWLEDGED = new WriteConcern(0);

    /**
     * Write operations wait for the server to flush the data to disk.
     *
     * @deprecated Prefer {@link #JOURNALED}
     */
    @Deprecated
    public static final WriteConcern FSYNCED = ACKNOWLEDGED.withFsync(true);

    /**
     * Write operations wait for the server to group commit to the journal file on disk.
     *
     * @mongodb.driver.manual core/write-concern/#journaled Journaled
     */
    public static final WriteConcern JOURNALED = ACKNOWLEDGED.withJournal(true);

    /**
     * Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.
     * @deprecated Prefer WriteConcern#W2
     */
    @Deprecated
    public static final WriteConcern REPLICA_ACKNOWLEDGED = new WriteConcern(2);

    /**
     * <p>Write operations that use this write concern will return as soon as the message is written to the socket. Exceptions are raised
     * for network issues, but not server errors.</p>
     *
     * <p>This field has been superseded by {@code WriteConcern.UNACKNOWLEDGED}, and may be deprecated in a future release.</p>
     *
     * @see WriteConcern#UNACKNOWLEDGED
     * @deprecated Prefer {@link #UNACKNOWLEDGED}
     */
    @Deprecated
    public static final WriteConcern NORMAL = UNACKNOWLEDGED;

    /**
     * <p>Write operations that use this write concern will wait for acknowledgement from the primary server before returning. Exceptions
     * are raised for network issues, and server errors.</p>
     *
     * <p>This field has been superseded by {@code WriteConcern.ACKNOWLEDGED}, and may be deprecated in a future release.</p>
     *
     * @see WriteConcern#ACKNOWLEDGED
     * @deprecated Prefer {@link #ACKNOWLEDGED}
     */
    @Deprecated
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
     * @deprecated Prefer {@link #JOURNALED}
     */
    @Deprecated
    public static final WriteConcern FSYNC_SAFE = FSYNCED;

    /**
     * <p>Exceptions are raised for network issues, and server errors; the write operation waits for the server to group commit to the
     * journal file on disk. </p>
     *
     * <p>This field has been superseded by {@code WriteConcern.JOURNALED}, and may be deprecated in a future release.</p>
     *
     * @deprecated Prefer {@link #JOURNALED}
     */
    @Deprecated
    public static final WriteConcern JOURNAL_SAFE = JOURNALED;

    /**
     * <p>Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write operation.</p>
     *
     * <p>This field has been superseded by {@code WriteConcern.REPLICA_ACKNOWLEDGED}, and may be deprecated in a future release.</p>
     *
     * @see WriteConcern#W2
     * @deprecated Prefer {@link #W2}
     */
    @Deprecated
    public static final WriteConcern REPLICAS_SAFE = REPLICA_ACKNOWLEDGED;

    /**
     * Constructs an instance with all properties initialized to their default values, except for w, which is initialized to 0, making
     * instances constructed this ways equivalent to {@code WriteConcern#UNACKNOWLEDGED}
     *
     * @see WriteConcern#UNACKNOWLEDGED
     * @deprecated Prefer {@link #UNACKNOWLEDGED}
     */
    @Deprecated
    public WriteConcern() {
        this(0);
    }

    /**
     * Construct an instance with the given integer-based value for w.
     *
     * @param w number of servers to ensure write propagation to before acknowledgment, which must be {@code >= 0}
     * @mongodb.driver.manual reference/write-concern/#w-option w option
     */
    public WriteConcern(final int w) {
        this(w, null, null, null);
    }

    /**
     * Construct an instance with the given tag set-based value for w.
     *
     * @param w tag set name, or "majority", representing the servers to ensure write propagation to before acknowledgment.  Do not use
     *          string representation of integer values for w
     * @mongodb.driver.manual tutorial/configure-replica-set-tag-sets/#replica-set-configuration-tag-sets Tag Sets
     * @mongodb.driver.manual reference/write-concern/#w-option w option
     */
    public WriteConcern(final String w) {
        this(w, null, null, null);
        notNull("w", w);
    }

    /**
     * Constructs an instance with the given integer-based value for w and the given value for wTimeoutMS.
     *
     * @param w          the w value, which must be &gt;= 0
     * @param wTimeoutMS the wTimeout in milliseconds, which must be &gt;= 0
     * @mongodb.driver.manual reference/write-concern/#w-option w option
     * @mongodb.driver.manual reference/write-concern/#wtimeout wtimeout option
     */
    public WriteConcern(final int w, final int wTimeoutMS) {
        this(w, wTimeoutMS, null, null);
    }

    /**
     * Constructs an instance with the given value for fsync.
     *
     * @param fsync whether or not to fsync
     * @deprecated prefer {@link #JOURNALED} or {@link #withJournal(Boolean)}
     */
    @Deprecated
    public WriteConcern(final boolean fsync) {
        this(null, null, fsync, null);
    }

    /**
     * Constructs an instance with the given integer-based w value, wTimeout in milliseconds, and fsync value.
     *
     * @param w          the w value, which must be &gt;= 0
     * @param wTimeoutMS the wTimeout in milliseconds, which must be &gt;= 0
     * @param fsync      whether or not to fsync
     * @deprecated Prefer {@link #withW(int)}, {@link #withWTimeout(long, TimeUnit)}, {@link #withJournal(Boolean)}
     * @mongodb.driver.manual reference/write-concern/#w-option w option
     * @mongodb.driver.manual reference/write-concern/#wtimeout wtimeout option
     */
    @Deprecated
    public WriteConcern(final int w, final int wTimeoutMS, final boolean fsync) {
        this(w, wTimeoutMS, fsync, null);
    }

    /**
     * Constructs an instance with the given integer-based w value, wTimeout in milliseconds, fsync value, and journal value.
     *
     * @param w          the w value, which must be &gt;= 0
     * @param wTimeoutMS the wTimeout in milliseconds, which must be &gt;= 0
     * @param fsync      whether or not to fsync
     * @param journal    whether writes should wait for a journaling group commit
     * @deprecated Prefer {@link #withW(int)}, {@link #withWTimeout(long, TimeUnit)}, {@link #withJournal(Boolean)}
     * @mongodb.driver.manual reference/write-concern/#w-option w option
     * @mongodb.driver.manual reference/write-concern/#wtimeout wtimeout option
     * @mongodb.driver.manual reference/write-concern/#j-option j option
     */
    @Deprecated
    public WriteConcern(final int w, final int wTimeoutMS, final boolean fsync, final boolean journal) {
        this((Object) w, wTimeoutMS, fsync, journal);
    }

    /**
     * Constructs an instance with the given String-based w value, wTimeout in milliseconds, fsync value, and journal value.
     *
     * @param w          the w value, which must be non-null
     * @param wTimeoutMS the wTimeout in milliseconds, which must be &gt;= 0
     * @param fsync      whether or not to fsync
     * @param journal    whether writes should wait for a journaling group commit
     * @deprecated Prefer {@link #withW(String)}, {@link #withWTimeout(long, TimeUnit)}, {@link #withJournal(Boolean)}
     */
    @Deprecated
    public WriteConcern(final String w, final int wTimeoutMS, final boolean fsync, final boolean journal) {
        this((Object) notNull("w", w), wTimeoutMS, fsync, journal);
    }

    // Private constructor for creating the "default" unacknowledged write concern.  Necessary because there already a no-args
    // constructor that means something else.
    private WriteConcern(@Nullable final Object w, @Nullable final Integer wTimeoutMS, @Nullable final Boolean fsync,
                         @Nullable final Boolean journal) {
        if (w instanceof Integer) {
            isTrueArgument("w >= 0", ((Integer) w) >= 0);
        } else if (w != null) {
            isTrueArgument("w must be String or int", w instanceof String);
        }
        isTrueArgument("wtimeout >= 0", wTimeoutMS == null || wTimeoutMS >= 0);
        this.w = w;
        this.wTimeoutMS = wTimeoutMS;
        this.fsync = fsync;
        this.journal = journal;
    }

    /**
     * Gets the w value.
     *
     * @return w, either an instance of Integer or String or null
     */
    public Object getWObject() {
        return w;
    }

    /**
     * Gets the w value as an integer.
     *
     * @return w as an int
     * @throws IllegalStateException if w is null or not an integer
     */
    public int getW() {
        isTrue("w is an Integer", w != null && w instanceof Integer);
        return (Integer) w;
    }

    /**
     * Gets the w parameter as a String.
     *
     * @return w as a String
     * @throws IllegalStateException if w is null or not a String
     */
    public String getWString() {
        isTrue("w is a String", w != null && w instanceof String);
        return (String) w;
    }

    /**
     * Gets the wTimeout in the given time unit.
     *
     * @param timeUnit the non-null time unit for the result
     * @return the WTimeout, which may be null if a wTimeout has not been specified
     * @since 3.2
     * @mongodb.driver.manual core/write-concern/#timeouts wTimeout
     */
    @Nullable
    public Integer getWTimeout(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return wTimeoutMS == null ? null : (int) timeUnit.convert(wTimeoutMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets the wTimeout in milliseconds.  If wTimeout has not been specified, returns 0.
     *
     * @return the wTimeout in milliseconds
     * @deprecated Prefer {@link #getWTimeout(TimeUnit)}
     * @mongodb.driver.manual core/write-concern/#timeouts wTimeout
     */
    @Deprecated
    public int getWtimeout() {
        return wTimeoutMS == null ? 0 : wTimeoutMS;
    }

    /**
     * Gets the journal property.  The default value is null.
     *
     * @return whether journal syncing is enabled, or null if unspecified.
     * @since 3.2
     * @mongodb.driver.manual core/write-concern/#journaled Journaled
     */
    public Boolean getJournal() {
        return journal;
    }

    /**
     * Gets the journal property.  If journal has not been specified, returns false.
     *
     * @return true if journal is true, false if it false or unspecified.
     * @mongodb.driver.manual core/write-concern/#journaled Journaled
     * @deprecated Prefer {@link #getJournal()}
     */
    @Deprecated
    public boolean getJ() {
        return journal == null ? false : journal;
    }

    /**
     * Gets the fsync property.  If fsync has not been specified, returns false.
     *
     * @return true if fsync is true, false if it false or unspecified
     * @deprecated Prefer {@link #getJournal()}
     */
    @Deprecated
    public boolean getFsync() {
        return fsync == null ? false : fsync;
    }

    /**
     * Gets the fsync property. If fsync has not been specified, returns false.
     *
     * @return true if fsync is true, false if it false or unspecified
     * @deprecated Prefer {@link #getJournal()}
     */
    @Deprecated
    public boolean fsync() {
        return getFsync();
    }

    /**
     * Returns true if this write concern indicates that write operations must be acknowledged.
     *
     * @return whether this write concern will result in an an acknowledged write
     * @deprecated Prefer {@link #isAcknowledged()}
     */
    @Deprecated
    public boolean callGetLastError() {
        return isAcknowledged();
    }

    /**
     * Gets whether this write concern indicates that the server's default write concern will be used.
     *
     * @return true if this write concern indicates that the server's default write concern will be used
     * @mongodb.driver.manual /reference/replica-configuration/#local.system.replset.settings.getLastErrorDefaults getLastErrorDefaults
     */
    public boolean isServerDefault() {
        return equals(ACKNOWLEDGED);
    }

    /**
     * Gets this write concern as a document.
     *
     * @return The write concern as a BsonDocument, even if {@code w &lt;= 0}
     */
    public BsonDocument asDocument() {
        BsonDocument document = new BsonDocument();

        addW(document);

        addWTimeout(document);
        addFSync(document);
        addJ(document);

        return document;
    }

    /**
     * Returns true if this write concern indicates that write operations must be acknowledged.
     *
     * @return true w != null or w &gt; 0 or journal is true or fsync is true
     * @mongodb.driver.manual core/write-concern/#acknowledged Acknowledged
     */
    public boolean isAcknowledged() {
        if (w instanceof Integer) {
            return (Integer) w > 0 || (journal != null && journal) || (fsync != null && fsync);
        }
        return true;
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

        if (w != null ? !w.equals(that.w) : that.w != null) {
            return false;
        }
        if (wTimeoutMS != null ? !wTimeoutMS.equals(that.wTimeoutMS) : that.wTimeoutMS != null) {
            return false;
        }
        if (fsync != null ? !fsync.equals(that.fsync) : that.fsync != null) {
            return false;
        }
        if (journal != null ? !journal.equals(that.journal) : that.journal != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = w != null ? w.hashCode() : 0;
        result = 31 * result + (wTimeoutMS != null ? wTimeoutMS.hashCode() : 0);
        result = 31 * result + (fsync != null ? fsync.hashCode() : 0);
        result = 31 * result + (journal != null ? journal.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WriteConcern{w=" + w + ", wTimeout=" + wTimeoutMS + " ms, fsync=" + fsync + ", journal=" + journal;

    }

    /**
     * Constructs a new WriteConcern from the current one and the specified integer-based value for w
     *
     * @param w number of servers to ensure write propagation to before acknowledgment, which must be {@code >= 0}
     * @return the new WriteConcern
     * @mongodb.driver.manual core/write-concern/#replica-acknowledged Replica Acknowledged
     */
    public WriteConcern withW(final int w) {
        return new WriteConcern(Integer.valueOf(w), wTimeoutMS, fsync, journal);
    }

    /**
     * Constructs a new WriteConcern from the current one and the specified tag-set based value for w
     *
     * @param w tag set, or "majority", representing the servers to ensure write propagation to before acknowledgment.  Do not use string
     *          representation of integer values for w
     * @return the new WriteConcern
     * @see #withW(int)
     * @mongodb.driver.manual tutorial/configure-replica-set-tag-sets/#replica-set-configuration-tag-sets Tag Sets
     */
    public WriteConcern withW(final String w) {
        notNull("w", w);
        return new WriteConcern(w, wTimeoutMS, fsync, journal);
    }

    /**
     * Constructs a new WriteConcern from the current one and the specified fsync value
     *
     * @param fsync true if the write concern needs to include fsync
     * @return the new WriteConcern
     * @deprecated Prefer {@link #withJournal(Boolean)}
     */
    @Deprecated
    public WriteConcern withFsync(final boolean fsync) {
        return new WriteConcern(w, wTimeoutMS, fsync, journal);
    }

    /**
     * Constructs a new WriteConcern from the current one and the specified journal value
     *
     * @param journal true if journalling is required for acknowledgement, false if not, or null if unspecified
     * @return the new WriteConcern
     * @since 3.2
     * @mongodb.driver.manual reference/write-concern/#j-option j option
     */
    public WriteConcern withJournal(final Boolean journal) {
        return new WriteConcern(w, wTimeoutMS, fsync, journal);
    }

    /**
     * Constructs a new WriteConcern from the current one and the specified journal value
     *
     * @param journal true if journalling is required for acknowledgement
     * @return the new WriteConcern
     * @deprecated Prefer {@link #withJournal(Boolean)}
     * @mongodb.driver.manual reference/write-concern/#j-option j option
     */
    @Deprecated
    public WriteConcern withJ(final boolean journal) {
        return withJournal(journal);
    }

    /**
     * Constructs a new WriteConcern from the current one and the specified wTimeout in the given time unit.
     *
     * @param wTimeout the wTimeout, which must be &gt;= 0 and &lt;= Integer.MAX_VALUE after conversion to milliseconds
     * @param timeUnit the non-null time unit to apply to wTimeout
     * @return the WriteConcern with the given wTimeout
     * @since 3.2
     * @mongodb.driver.manual reference/write-concern/#wtimeout wtimeout option
     */
    public WriteConcern withWTimeout(final long wTimeout, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        long newWTimeOutMS = TimeUnit.MILLISECONDS.convert(wTimeout, timeUnit);
        isTrueArgument("wTimeout >= 0", wTimeout >= 0);
        isTrueArgument("wTimeout <= " + Integer.MAX_VALUE + " ms", newWTimeOutMS <= Integer.MAX_VALUE);
        return new WriteConcern(w, (int) newWTimeOutMS, fsync, journal);
    }

    private void addW(final BsonDocument document) {
        if (w instanceof String) {
            document.put("w", new BsonString((String) w));
        } else if (w instanceof Integer){
            document.put("w", new BsonInt32((Integer) w));
        }
    }

    private void addJ(final BsonDocument document) {
        if (journal != null) {
            document.put("j", BsonBoolean.valueOf(journal));
        }
    }

    private void addFSync(final BsonDocument document) {
        if (fsync != null) {
            document.put("fsync", BsonBoolean.valueOf(fsync));
        }
    }

    private void addWTimeout(final BsonDocument document) {
        if (wTimeoutMS != null) {
            document.put("wtimeout", new BsonInt32(wTimeoutMS));
        }
    }

    /**
     * Create a Majority Write Concern that requires a majority of servers to acknowledge the write.
     *
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     * @param j        whether writes should wait for a journal group commit
     * @return Majority, a subclass of WriteConcern that represents the write concern requiring most servers to acknowledge the write
     * @deprecated Prefer {@link #MAJORITY}, {@link #withWTimeout(long, TimeUnit)}, {@link #withJournal(Boolean)}
     */
    @Deprecated
    public static Majority majorityWriteConcern(final int wtimeout, final boolean fsync, final boolean j) {
        return new Majority(wtimeout, fsync, j);
    }

    /**
     * A write concern that blocks acknowledgement of a write operation until a majority of replica set members have applied it.
     *
     * @deprecated Prefer {@link #MAJORITY}, {@link #withWTimeout(long, TimeUnit)}, {@link #withJournal(Boolean)}
     */
    @Deprecated
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
