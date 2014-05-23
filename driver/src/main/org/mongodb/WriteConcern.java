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

package org.mongodb;

import org.mongodb.annotations.Immutable;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import static org.mongodb.assertions.Assertions.isTrueArgument;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * <p>WriteConcern control the acknowledgment of write operations with various options. <p> <b>w</b> <ul> <li>-1 = Don't even report network
 * errors </li> <li> 0 = Don't wait for acknowledgement from the server </li> <li> 1 = Wait for acknowledgement, but don't wait for
 * secondaries to replicate</li> <li> 2+= Wait for one or more secondaries to also acknowledge </li> </ul> <b>wtimeout</b> how long to wait
 * for slaves before failing <ul> <li>0: indefinite </li> <li>greater than 0: ms to wait </li> </ul> </p>
 * <p/>
 * Other options: <ul> <li><b>j</b>: wait for group commit to journal</li> <li><b>fsync</b>: force fsync to disk</li> </ul>
 *
 * @mongodb.driver.manual core/write-operations/#write-concern Write Concern
 */
@Immutable
public class WriteConcern implements Serializable {

    private static final long serialVersionUID = 1884671104750417011L;

    // map of the constants from above for use by valueOf
    private static final Map<String, WriteConcern> NAMED_CONCERNS;

    private final Object w;

    private final int wtimeout;

    private final boolean fsync;

    private final boolean j;

    /**
     * Write operations that use this write concern will wait for acknowledgement from the primary server before returning. Exceptions are
     * raised for network issues, and server errors.
     */
    public static final WriteConcern ACKNOWLEDGED = new WriteConcern(1);

    /**
     * Write operations that use this write concern will return as soon as the message is written to the socket. Exceptions are raised for
     * network issues, but not server errors.
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
     * Gets the WriteConcern constants by name (matching is done case insensitively).
     *
     * @param name the name of the write concern
     * @return the WriteConcern that matches the given name
     */
    public static WriteConcern valueOf(final String name) {
        return NAMED_CONCERNS.get(name.toLowerCase());
    }

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
     * @param w               number of writes
     * @param wtimeout        timeout for write operation
     * @param fsync           whether or not to fsync
     * @param j               whether writes should wait for a journaling group commit
     */
    public WriteConcern(final int w, final int wtimeout, final boolean fsync, final boolean j) {
        isTrueArgument("w >= 0", w >= 0);
        this.w = w;
        this.wtimeout = wtimeout;
        this.fsync = fsync;
        this.j = j;
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
     */
    public WriteConcern(final String w, final int wtimeout, final boolean fsync, final boolean j) {
        this.w = notNull("w", w);
        this.wtimeout = wtimeout;
        this.fsync = fsync;
        this.j = j;
    }

    /**
     * Gets this write concern as a document
     *
     * @return The write concern as a Document, even if {@code w <= 0}
     */
    public Document asDocument() {
        if (!isAcknowledged()) {
            throw new IllegalStateException("The write is unacknowledged, so no document can be created");
        }
        Document document = new Document();

        document.put("w", w);

        addWTimeout(document);
        addFSync(document);
        addJ(document);

        return document;
    }

    /**
     * The server default is w == 1 and everything else the default value.
     * @mongodb.driver.manual /reference/replica-configuration/#local.system.replset.settings.getLastErrorDefaults getLastErrorDefaults
     * @return true if this write concern is the server's default
     */
    public boolean isServerDefault() {
        return w.equals(1) && wtimeout == 0 && !fsync && !j;
    }

    private void addJ(final Document document) {
        if (j) {
            document.put("j", true);
        }
    }

    private void addFSync(final Document document) {
        if (fsync) {
            document.put("fsync", true);
        }
    }

    private void addWTimeout(final Document document) {
        if (wtimeout > 0) {
            document.put("wtimeout", wtimeout);
        }
    }

    /**
     * Gets the w value (the write strategy)
     *
     * @return the WriteConcern as an object
     */
    public Object getWObject() {
        return w;
    }

    /**
     * Gets the w parameter (the write strategy)
     *
     * @return w
     */
    public int getW() {
        return (Integer) w;
    }

    /**
     * Gets the w parameter (the write strategy) in String format
     *
     * @return w as a string
     */
    public String getWString() {
        return (String) w;
    }

    /**
     * Gets the write timeout (in milliseconds)
     *
     * @return the the write timeout
     */
    public int getWtimeout() {
        return wtimeout;
    }

    /**
     * Gets the fsync flag (fsync to disk on the server)
     *
     * @return true if fsync set
     */
    public boolean getFsync() {
        return fsync;
    }

    /**
     * Gets the j parameter (journal syncing)
     *
     * @return true if j is set
     */
    public boolean getJ() {
        return j;
    }

    //CHECKSTYLE:OFF

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

    //CHECKSTYLE:ON

    /**
     * Returns whether write operations should be acknowledged
     *
     * @return true w != null or w > 0
     */
    public boolean isAcknowledged() {
        if (w instanceof Integer) {
            return (Integer) w > 0;
        }
        return w != null;
    }


    @Override
    public String toString() {
        return "WriteConcern{w=" + w + ", wtimeout=" + wtimeout + ", fsync=" + fsync + ", j=" + j;

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
