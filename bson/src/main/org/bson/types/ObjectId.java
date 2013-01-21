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

package org.bson.types;

import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A globally unique identifier for objects. <p>Consists of 12 bytes, divided as follows:
 * <blockquote><pre>
 * <table border="1">
 * <tr><td>0</td><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td><td>6</td>
 *     <td>7</td><td>8</td><td>9</td><td>10</td><td>11</td></tr>
 * <tr><td colspan="4">time</td><td colspan="3">machine</td>
 *     <td colspan="2">pid</td><td colspan="3">inc</td></tr>
 * </table>
 * </pre></blockquote>
 *
 * @dochub objectids
 */
public class ObjectId implements Comparable<ObjectId>, java.io.Serializable {

    private static final long serialVersionUID = -4415279469780082174L;

    static final Logger LOGGER = Logger.getLogger("org.bson.ObjectId");

    /**
     * Gets a new object id.
     *
     * @return the new id
     */
    public static ObjectId get() {
        return new ObjectId();
    }

    /**
     * Checks if a string could be an <code>ObjectId</code>.
     *
     * @return whether the string could be an object id
     */
    public static boolean isValid(final String s) {
        if (s == null) {
            return false;
        }

        final int len = s.length();
        if (len != 24) {
            return false;
        }

        for (int i = 0; i < len; i++) {
            final char c = s.charAt(i);
            if (c >= '0' && c <= '9') {
                continue;
            }
            if (c >= 'a' && c <= 'f') {
                continue;
            }
            if (c >= 'A' && c <= 'F') {
                continue;
            }

            return false;
        }

        return true;
    }

    /**
     * Turn an object into an <code>ObjectId</code>, if possible. Strings will be converted into <code>ObjectId</code>s,
     * if possible, and <code>ObjectId</code>s will be cast and returned.  Passing in <code>null</code> returns
     * <code>null</code>.
     *
     * @param o the object to convert
     * @return an <code>ObjectId</code> if it can be massaged, null otherwise
     */
    public static ObjectId massageToObjectId(final Object o) {
        if (o == null) {
            return null;
        }

        if (o instanceof ObjectId) {
            return (ObjectId) o;
        }

        if (o instanceof String) {
            final String s = o.toString();
            if (isValid(s)) {
                return new ObjectId(s);
            }
        }

        return null;
    }

    public ObjectId(final Date time) {
        this(time, GENMACHINE, NEXT_INC.getAndIncrement());
    }

    public ObjectId(final Date time, final int inc) {
        this(time, GENMACHINE, inc);
    }

    public ObjectId(final Date time, final int machine, final int inc) {
        this.time = (int) (time.getTime() / 1000);
        this.machine = machine;
        this.inc = inc;
        isNew = false;
    }

    /**
     * Creates a new instance from a string.
     *
     * @param s the string to convert
     * @throws IllegalArgumentException if the string is not a valid id
     */
    public ObjectId(final String s) {
        this(s, false);
    }

    public ObjectId(final String s, final boolean babble) {
        String str = s;

        if (!isValid(s)) {
            throw new IllegalArgumentException("invalid ObjectId [" + s + "]");
        }

        if (babble) {
            str = babbleToMongod(s);
        }

        final byte[] b = new byte[12];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) Integer.parseInt(str.substring(i * 2, i * 2 + 2), 16);
        }
        final ByteBuffer bb = ByteBuffer.wrap(b);
        time = bb.getInt();
        machine = bb.getInt();
        inc = bb.getInt();
        isNew = false;
    }

    public ObjectId(final byte[] b) {
        if (b.length != 12) {
            throw new IllegalArgumentException("need 12 bytes");
        }
        final ByteBuffer bb = ByteBuffer.wrap(b);
        time = bb.getInt();
        machine = bb.getInt();
        inc = bb.getInt();
        isNew = false;
    }

    /**
     * Creates an ObjectId
     *
     * @param time    time in seconds
     * @param machine machine ID
     * @param inc     incremental value
     */
    public ObjectId(final int time, final int machine, final int inc) {
        this.time = time;
        this.machine = machine;
        this.inc = inc;
        isNew = false;
    }

    /**
     * Create a new object id.
     */
    public ObjectId() {
        time = (int) (System.currentTimeMillis() / 1000);
        machine = GENMACHINE;
        inc = NEXT_INC.getAndIncrement();
        isNew = true;
    }

    public int hashCode() {
        int x = time;
        x += (machine * 111);
        x += (inc * 17);
        return x;
    }

    public boolean equals(final Object o) {

        if (this == o) {
            return true;
        }

        final ObjectId other = massageToObjectId(o);
        if (other == null) {
            return false;
        }

        return time == other.time && machine == other.machine && inc == other.inc;
    }

    public String toStringBabble() {
        return babbleToMongod(toStringMongod());
    }

    public String toStringMongod() {
        final byte[] b = toByteArray();

        final StringBuilder buf = new StringBuilder(24);

        for (int i = 0; i < b.length; i++) {
            final int x = b[i] & 0xFF;
            final String s = Integer.toHexString(x);
            if (s.length() == 1) {
                buf.append("0");
            }
            buf.append(s);
        }

        return buf.toString();
    }

    public byte[] toByteArray() {
        final byte[] b = new byte[12];
        final ByteBuffer bb = ByteBuffer.wrap(b);
        // by default BB is big endian like we need
        bb.putInt(time);
        bb.putInt(machine);
        bb.putInt(inc);
        return b;
    }

    static String pos(final String s, final int p) {
        return s.substring(p * 2, (p * 2) + 2);
    }

    public static String babbleToMongod(final String b) {
        if (!isValid(b)) {
            throw new IllegalArgumentException("invalid object id: " + b);
        }

        final StringBuilder buf = new StringBuilder(24);
        for (int i = 7; i >= 0; i--) {
            buf.append(pos(b, i));
        }
        for (int i = 11; i >= 8; i--) {
            buf.append(pos(b, i));
        }

        return buf.toString();
    }

    public String toString() {
        return toStringMongod();
    }

    int compareUnsigned(final int i, final int j) {
        long li = 0xFFFFFFFFL;
        li = i & li;
        long lj = 0xFFFFFFFFL;
        lj = j & lj;
        final long diff = li - lj;
        if (diff < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        if (diff > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) diff;
    }

    public int compareTo(final ObjectId id) {
        if (id == null) {
            return -1;
        }

        int x = compareUnsigned(time, id.time);
        if (x != 0) {
            return x;
        }

        x = compareUnsigned(machine, id.machine);
        if (x != 0) {
            return x;
        }

        return compareUnsigned(inc, id.inc);
    }

    public int getMachine() {
        return machine;
    }

    /**
     * Gets the time of this ID, in milliseconds
     */
    public long getTime() {
        return time * 1000L;
    }

    /**
     * Gets the time of this ID, in seconds
     */
    public int getTimeSecond() {
        return time;
    }

    public int getInc() {
        return inc;
    }

    public int time() {
        return time;
    }

    public int machine() {
        return machine;
    }

    public int inc() {
        return inc;
    }

    public boolean isNew() {
        return isNew;
    }

    public void notNew() {
        isNew = false;
    }

    /**
     * Gets the generated machine ID, identifying the machine / process / class loader
     */
    public static int getGenMachineId() {
        return GENMACHINE;
    }

    /**
     * Gets the current value of the auto increment
     */
    public static int getCurrentInc() {
        return NEXT_INC.get();
    }

    private final int time;
    private final int machine;
    private final int inc;

    private boolean isNew;

    public static int flip(final int x) {
        int z = 0;
        z |= ((x << 24) & 0xFF000000);
        z |= ((x << 8) & 0x00FF0000);
        z |= ((x >> 8) & 0x0000FF00);
        z |= ((x >> 24) & 0x000000FF);
        return z;
    }

    private static final AtomicInteger NEXT_INC = new AtomicInteger((new java.util.Random()).nextInt());

    private static final int GENMACHINE;

    static {
        try {
            // build a 2-byte machine piece based on NICs info
            int machinePiece;
                try {
                    final StringBuilder sb = new StringBuilder();
                    final Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
                    while (e.hasMoreElements()) {
                        final NetworkInterface ni = e.nextElement();
                        sb.append(ni.toString());
                    }
                    machinePiece = sb.toString().hashCode() << 16;
                } catch (Throwable e) {
                    // exception sometimes happens with IBM JVM, use random
                    LOGGER.log(Level.WARNING, e.getMessage(), e);
                    machinePiece = (new Random().nextInt()) << 16;
                }
                LOGGER.fine("machine piece post: " + Integer.toHexString(machinePiece));
            LOGGER.fine("machine piece post: " + Integer.toHexString(machinePiece));

            // add a 2 byte process piece. It must represent not only the JVM but the class loader.
            // Since static var belong to class loader there could be collisions otherwise
            final int processPiece;
            int processId = new java.util.Random().nextInt();
            try {
                processId = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().hashCode();
            } catch (Throwable t) {
                //silently fail??
                LOGGER.log(Level.WARNING, t.getMessage(), t);
            }

            final ClassLoader loader = ObjectId.class.getClassLoader();
            final int loaderId = loader != null ? System.identityHashCode(loader) : 0;

            final StringBuilder sb = new StringBuilder();
            sb.append(Integer.toHexString(processId));
            sb.append(Integer.toHexString(loaderId));
            processPiece = sb.toString().hashCode() & 0xFFFF;
            LOGGER.fine("process piece: " + Integer.toHexString(processPiece));

            GENMACHINE = machinePiece | processPiece;
            LOGGER.fine("machine : " + Integer.toHexString(GENMACHINE));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}

