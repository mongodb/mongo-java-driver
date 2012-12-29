/*
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
 */

package org.mongodb;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>WriteConcern control the acknowledgment of write operations with various options. <p> <b>w</b> <ul> <li>-1 = Don't
 * even report network errors </li> <li> 0 = Don't wait for acknowledgement from the server </li> <li> 1 = Wait for
 * acknowledgement, but don't wait for secondaries to replicate</li> <li> 2+= Wait for one or more secondaries to also
 * acknowledge </li> </ul> <b>wtimeout</b> how long to wait for slaves before failing <ul> <li>0: indefinite </li>
 * <li>greater than 0: ms to wait </li> </ul> </p>
 * <p/>
 * Other options: <ul> <li><b>j</b>: wait for group commit to journal</li> <li><b>fsync</b>: force fsync to disk</li>
 * </ul>
 *
 * @dochub databases
 */
public class WriteConcern implements Serializable {

    private static final long serialVersionUID = 1884671104750417011L;

    // map of the constants from above for use by valueOf
    private static final Map<String, WriteConcern> _namedConcerns;

    private final Object w;

    private final int wtimeout;

    private final boolean fsync;

    private final boolean j;

    private final boolean continueOnErrorForInsert;

    /**
     * No exceptions are raised, even for network issues.
     */
    public static final WriteConcern ERRORS_IGNORED = new WriteConcern(-1);

    /**
     * Write operations that use this write concern will wait for acknowledgement from the primary server before
     * returning. Exceptions are raised for network issues, and server errors.
     */
    public static final WriteConcern ACKNOWLEDGED = new WriteConcern(1);

    /**
     * Write operations that use this write concern will return as soon as the message is written to the socket.
     * Exceptions are raised for network issues, but not server errors.
     */
    public static final WriteConcern UNACKNOWLEDGED = new WriteConcern(0);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to flush
     * the data to disk.
     */
    public static final WriteConcern FSYNCED = new WriteConcern(true);

    /**
     * Exceptions are raised for network issues, and server errors; the write operation waits for the server to group
     * commit to the journal file on disk.
     */
    public static final WriteConcern JOURNALED = new WriteConcern(1, 0, false, true);

    /**
     * Exceptions are raised for network issues, and server errors; waits for at least 2 servers for the write
     * operation.
     */
    public static final WriteConcern REPLICA_ACKNOWLEDGED = new WriteConcern(2);

    /**
     * Gets the WriteConcern constants by name (matching is done case insensitively).
     *
     * @param name
     * @return
     */
    public static WriteConcern valueOf(final String name) {
        return _namedConcerns.get(name.toLowerCase());
    }

    /**
     * Default constructor keeping all options as default.  Be careful using this constructor, as it's equivalent to
     * {@code WriteConcern.UNACKNOWLEDGED}, so writes may be lost without any errors being reported.
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
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and
     * exception raising behavior </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking
     * is done</li> <li>{@code w=0} None, network socket errors raised</li> <li>{@code w=1} Checks server for errors as
     * well as network socket errors raised</li> <li>{@code w>1} Checks servers (w) for errors as well as network socket
     * errors raised</li> </ul> </p>
     *
     * @param w        number of writes
     * @param wtimeout timeout for write operation
     * @param fsync    whether or not to fsync
     */
    public WriteConcern(final int w, final int wtimeout, final boolean fsync) {
        this(w, wtimeout, fsync, false);
    }

    /**
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and
     * exception raising behavior </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking
     * is done</li> <li>{@code w=0} None, network socket errors raised</li> <li>{@code w=1} Checks server for errors as
     * well as network socket errors raised</li> <li>{@code w>1} Checks servers (w) for errors as well as network socket
     * errors raised</li> </ul> </p>
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
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and
     * exception raising behavior </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking
     * is done</li> <li>{@code w=0} None, network socket errors raised</li> <li>{@code w=1} Checks server for errors as
     * well as network socket errors raised</li> <li>{@code w>1} Checks servers (w) for errors as well as network socket
     * errors raised</li> </ul> </p>
     *
     * @param w                     number of writes
     * @param wtimeout              timeout for write operation
     * @param fsync                 whether or not to fsync
     * @param j                     whether writes should wait for a journaling group commit
     * @param continueOnErrorForInsert if batch inserts should continue after the first error
     */
    public WriteConcern(final int w, final int wtimeout, final boolean fsync, final boolean j, final boolean continueOnErrorForInsert) {
        this.w = w;
        this.wtimeout = wtimeout;
        this.fsync = fsync;
        this.j = j;
        this.continueOnErrorForInsert = continueOnErrorForInsert;
    }

    /**
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and
     * exception raising behavior </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking
     * is done</li> <li>{@code w=0} None, network socket errors raised</li> <li>{@code w=1} Checks server for errors as
     * well as network socket errors raised</li> <li>{@code w>1} Checks servers (w) for errors as well as network socket
     * errors raised</li> </ul> </p>
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
     * Creates a WriteConcern object. <p>Specifies the number of servers to wait for on the write operation, and
     * exception raising behavior </p> <p> w represents the number of servers: <ul> <li>{@code w=-1} None, no checking
     * is done</li> <li>{@code w=0} None, network socket errors raised</li> <li>{@code w=1} Checks server for errors as
     * well as network socket errors raised</li> <li>{@code w>1} Checks servers (w) for errors as well as network socket
     * errors raised</li> </ul> </p>
     *
     * @param w                     number of writes
     * @param wtimeout              timeout for write operation
     * @param fsync                 whether or not to fsync
     * @param j                     whether writes should wait for a journaling group commit
     * @param continueOnErrorForInsert if batch inserts should continue after the first error
     */
    public WriteConcern(final String w, final int wtimeout, final boolean fsync, final boolean j, final boolean continueOnErrorForInsert) {
        if (w == null) {
            throw new IllegalArgumentException("w can not be null");
        }

        this.w = w;
        this.wtimeout = wtimeout;
        this.fsync = fsync;
        this.j = j;
        this.continueOnErrorForInsert = continueOnErrorForInsert;
    }

    /**
     * Gets the getlasterror command for this write concern.
     *
     * @return getlasterror command, even if <code>w <= 0</code>
     */
    public CommandDocument getCommand() {
        final CommandDocument command = new CommandDocument("getlasterror", 1);

        if (w instanceof Integer && ((Integer) w > 1) || (w instanceof String)) {
            command.put("w", w);
        }

        if (wtimeout > 0) {
            command.put("wtimeout", wtimeout);
        }

        if (fsync) {
            command.put("fsync", true);
        }

        if (j) {
            command.put("j", true);
        }

        return command;
    }

    /**
     * Gets the w value (the write strategy)
     *
     * @return
     */
    public Object getWObject() {
        return w;
    }

    /**
     * Gets the w parameter (the write strategy)
     *
     * @return
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
     * @return
     */
    public int getWtimeout() {
        return wtimeout;
    }

    /**
     * Gets the fsync flag (fsync to disk on the server)
     *
     * @return
     */
    public boolean getFsync() {
        return fsync;
    }

    /**
     * Gets the j parameter (journal syncing)
     *
     * @return
     */
    public boolean getJ() {
        return j;
    }

    /**
     * Gets the "continue inserts on error" mode
     *
     * @return
     */
    public boolean getContinueOnErrorForInsert() {
        return continueOnErrorForInsert;
    }

    /**
     *
     * @param w
     * @return
     */
    public WriteConcern withW(int w) {
        return new WriteConcern(w, getWtimeout(), getFsync(), getJ(), getContinueOnErrorForInsert());
    }

    /**
     *
     * @param w
     * @return
     */
    public WriteConcern withW(String w) {
        return new WriteConcern(w, getWtimeout(), getFsync(), getJ(), getContinueOnErrorForInsert());
    }

    /**
     *
     * @param fsync
     * @return
     */
    public WriteConcern withFsync(boolean fsync) {
        if (getWObject() instanceof Integer) {
            return new WriteConcern(getW(), getWtimeout(), fsync, getJ(), getContinueOnErrorForInsert());
        }
        else {
            return new WriteConcern(getWString(), getWtimeout(), fsync, getJ(), getContinueOnErrorForInsert());
        }
    }

    /**
     *
     * @param j
     * @return
     */
    public WriteConcern withJ(boolean j) {
        if (getWObject() instanceof Integer) {
            return new WriteConcern(getW(), getWtimeout(), getFsync(), j, getContinueOnErrorForInsert());
        }
        else {
            return new WriteConcern(getWString(), getWtimeout(), getFsync(), j, getContinueOnErrorForInsert());
        }
    }

    /**
     *
     * @param continueOnErrorForInsert
     * @return
     */
    public WriteConcern withContinueOnErrorForInsert(boolean continueOnErrorForInsert) {
        if (getWObject() instanceof Integer) {
            return new WriteConcern(getW(), getWtimeout(), getFsync(), getJ(), continueOnErrorForInsert);
        }
        else {
            return new WriteConcern(getWString(), getWtimeout(), getFsync(), getJ(), continueOnErrorForInsert);
        }
    }

    /**
     * Returns whether network error may be raised (w >= 0)
     *
     * @return
     */
    public boolean raiseNetworkErrors() {
        if (w instanceof Integer) {
            return (Integer) w >= 0;
        }
        return w != null;
    }

    /**
     * Returns whether "getlasterror" should be called (w > 0)
     *
     * @return
     */
    public boolean callGetLastError() {
        if (w instanceof Integer) {
            return (Integer) w > 0;
        }
        return w != null;
    }


    @Override
    public String toString() {
        return "WriteConcern{" +
                "w=" + w +
                ", wtimeout=" + wtimeout +
                ", fsync=" + fsync +
                ", j=" + j +
                ", continueOnErrorForInsert=" + continueOnErrorForInsert +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final WriteConcern that = (WriteConcern) o;

        if (continueOnErrorForInsert != that.continueOnErrorForInsert) {
            return false;
        }
        if (fsync != that.fsync) {
            return false;
        }
        if (j != that.j) {
            return false;
        }
        if (wtimeout != that.wtimeout) {
            return false;
        }
        if (!w.equals(that.w)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = w.hashCode();
        result = 31 * result + wtimeout;
        result = 31 * result + (fsync ? 1 : 0);
        result = 31 * result + (j ? 1 : 0);
        result = 31 * result + (continueOnErrorForInsert ? 1 : 0);
        return result;
    }

    static {
        _namedConcerns = new HashMap<String, WriteConcern>();
        for (Field f : WriteConcern.class.getFields()) {
            if (Modifier.isStatic(f.getModifiers()) && f.getType().equals(WriteConcern.class)) {
                String key = f.getName().toLowerCase();
                try {
                    _namedConcerns.put(key, (WriteConcern) f.get(null));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
