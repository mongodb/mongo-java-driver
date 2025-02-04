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

package org.bson.types;

import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>A globally unique identifier for objects.</p>
 *
 * <p>Consists of 12 bytes, divided as follows:</p>
 * <table border="1">
 * <caption>ObjectID layout</caption>
 * <tr>
 * <td>0</td><td>1</td><td>2</td><td>3</td><td>4</td><td>5</td><td>6</td><td>7</td><td>8</td><td>9</td><td>10</td><td>11</td>
 * </tr>
 * <tr>
 * <td colspan="4">time</td><td colspan="5">random value</td><td colspan="3">inc</td>
 * </tr>
 * </table>
 *
 * <p>Instances of this class are immutable.</p>
 *
 * @mongodb.driver.manual core/object-id ObjectId
 */
public final class ObjectId implements Comparable<ObjectId>, Serializable {

    // unused, as this class uses a proxy for serialization
    private static final long serialVersionUID = 1L;

    private static final int OBJECT_ID_LENGTH = 12;
    private static final int LOW_ORDER_THREE_BYTES = 0x00ffffff;

    // Use upper bytes of a long to represent the 5-byte random value.
    private static final long RANDOM_VALUE;

    private static final AtomicInteger NEXT_COUNTER;

    private static final char[] HEX_CHARS = {
            '0', '1', '2', '3', '4', '5', '6', '7',
            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * The timestamp
     */
    private final int timestamp;

    /**
     * The final 8 bytes of the ObjectID are 5 bytes probabilistically unique to the machine and
     * process, followed by a 3 byte incrementing counter initialized to a random value.
     */
    private final long nonce;

    /**
     * Gets a new object id.
     *
     * @return the new id
     */
    public static ObjectId get() {
        return new ObjectId();
    }

    /**
     * Gets a new object id with the given date value and all other bits zeroed.
     * <p>
     * The returned object id will compare as less than or equal to any other object id within the same second as the given date, and
     * less than any later date.
     * </p>
     *
     * @param date the date
     * @return the ObjectId
     * @since 4.1
     */
    public static ObjectId getSmallestWithDate(final Date date) {
        return new ObjectId(dateToTimestampSeconds(date), 0L);
    }

    /**
     * Checks if a string could be an {@code ObjectId}.
     *
     * @param hexString a potential ObjectId as a String.
     * @return whether the string could be an object id
     * @throws IllegalArgumentException if hexString is null
     */
    public static boolean isValid(final String hexString) {
        if (hexString == null) {
            throw new IllegalArgumentException();
        }

        int len = hexString.length();
        if (len != 24) {
            return false;
        }

        for (int i = 0; i < len; i++) {
            char c = hexString.charAt(i);
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
     * Create a new object id.
     */
    public ObjectId() {
        this(new Date());
    }

    /**
     * Constructs a new instance using the given date.
     *
     * @param date the date
     */
    public ObjectId(final Date date) {
        this(dateToTimestampSeconds(date), RANDOM_VALUE | (NEXT_COUNTER.getAndIncrement() & LOW_ORDER_THREE_BYTES));
    }

    /**
     * Constructs a new instances using the given date and counter.
     *
     * @param date    the date
     * @param counter the counter
     * @throws IllegalArgumentException if the high order byte of counter is not zero
     */
    public ObjectId(final Date date, final int counter) {
        this(dateToTimestampSeconds(date), getNonceFromUntrustedCounter(counter));
    }

    /**
     * Creates an ObjectId using the given time and counter.
     *
     * @param timestamp the time in seconds
     * @param counter   the counter
     * @throws IllegalArgumentException if the high order byte of counter is not zero
     */
    public ObjectId(final int timestamp, final int counter) {
        this(timestamp, getNonceFromUntrustedCounter(counter));
    }

    private ObjectId(final int timestamp, final long nonce) {
        this.timestamp = timestamp;
        this.nonce = nonce;
    }

    private static long getNonceFromUntrustedCounter(final int counter) {
        if ((counter & 0xff000000) != 0) {
            throw new IllegalArgumentException("The counter must be between 0 and 16777215 (it must fit in three bytes).");
        }
        return RANDOM_VALUE | counter;
    }

    /**
     * Constructs a new instance from a 24-byte hexadecimal string representation.
     *
     * @param hexString the string to convert
     * @throws IllegalArgumentException if the string is not a valid hex string representation of an ObjectId
     */
    public ObjectId(final String hexString) {
        this(parseHexString(hexString));
    }

    /**
     * Constructs a new instance from the given byte array
     *
     * @param bytes the byte array
     * @throws IllegalArgumentException if array is null or not of length 12
     */
    public ObjectId(final byte[] bytes) {
        this(ByteBuffer.wrap(isTrueArgument("bytes has length of 12", bytes, notNull("bytes", bytes).length == 12)));
    }

    /**
     * Constructs a new instance from the given ByteBuffer
     *
     * @param buffer the ByteBuffer
     * @throws IllegalArgumentException if the buffer is null or does not have at least 12 bytes remaining
     * @since 3.4
     */
    public ObjectId(final ByteBuffer buffer) {
        notNull("buffer", buffer);
        isTrueArgument("buffer.remaining() >=12", buffer.remaining() >= OBJECT_ID_LENGTH);

       ByteOrder originalOrder = buffer.order();
        try {
            buffer.order(ByteOrder.BIG_ENDIAN);
            this.timestamp = buffer.getInt();
            this.nonce = buffer.getLong();
        } finally {
            buffer.order(originalOrder);
        }
    }

    /**
     * Convert to a byte array.  Note that the numbers are stored in big-endian order.
     *
     * @return the byte array
     */
    public byte[] toByteArray() {
        // using .allocate ensures there is a backing array that can be returned
        return ByteBuffer.allocate(OBJECT_ID_LENGTH).putInt(this.timestamp).putLong(this.nonce).array();
    }

    /**
     * Convert to bytes and put those bytes to the provided ByteBuffer.
     * Note that the numbers are stored in big-endian order.
     *
     * @param buffer the ByteBuffer
     * @throws IllegalArgumentException if the buffer is null or does not have at least 12 bytes remaining
     * @since 3.4
     */
    public void putToByteBuffer(final ByteBuffer buffer) {
        notNull("buffer", buffer);
        isTrueArgument("buffer.remaining() >=12", buffer.remaining() >= OBJECT_ID_LENGTH);

       ByteOrder originalOrder = buffer.order();
        try{
            buffer.order(ByteOrder.BIG_ENDIAN);
            buffer.putInt(this.timestamp);
            buffer.putLong(this.nonce);
        } finally {
            buffer.order(originalOrder);
        }
    }

    /**
     * Gets the timestamp (number of seconds since the Unix epoch).
     *
     * @return the timestamp
     */
    public int getTimestamp() {
        return timestamp;
    }

    /**
     * Gets the timestamp as a {@code Date} instance.
     *
     * @return the Date
     */
    public Date getDate() {
        return new Date((timestamp & 0xFFFFFFFFL) * 1000L);
    }

    /**
     * Converts this instance into a 24-byte hexadecimal string representation.
     *
     * @return a string representation of the ObjectId in hexadecimal format
     */
    public String toHexString() {
        char[] chars = new char[OBJECT_ID_LENGTH * 2];
        int i = 0;
        for (byte b : toByteArray()) {
            chars[i++] = HEX_CHARS[b >> 4 & 0xF];
            chars[i++] = HEX_CHARS[b & 0xF];
        }
        return new String(chars);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ObjectId other = (ObjectId) o;
        if (timestamp != other.timestamp) {
            return false;
        }
        return nonce == other.nonce;
    }

    @Override
    public int hashCode() {
        return 31 * timestamp + Long.hashCode(nonce);
    }

    @Override
    public int compareTo(final ObjectId other) {
        int cmp = Integer.compareUnsigned(this.timestamp, other.timestamp);
        if (cmp != 0) {
            return cmp;
        }

        return Long.compareUnsigned(nonce, other.nonce);
    }

    @Override
    public String toString() {
        return toHexString();
    }

    /**
     * Write the replacement object.
     *
     * <p>
     * See https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
     * </p>
     *
     * @return a proxy for the document
     */
    private Object writeReplace() {
        return new SerializationProxy(this);
    }

    /**
     * Prevent normal deserialization.
     *
     * <p>
     * See https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
     * </p>
     *
     * @param stream the stream
     * @throws InvalidObjectException in all cases
     */
    private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
        throw new InvalidObjectException("Proxy required");
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 1L;

        private final byte[] bytes;

        SerializationProxy(final ObjectId objectId) {
            bytes = objectId.toByteArray();
        }

        private Object readResolve() {
            return new ObjectId(bytes);
        }
    }

    static {
        try {
            SecureRandom secureRandom = new SecureRandom();
            RANDOM_VALUE = secureRandom.nextLong() & ~LOW_ORDER_THREE_BYTES;
            NEXT_COUNTER = new AtomicInteger(secureRandom.nextInt());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static byte[] parseHexString(final String s) {
        notNull("hexString", s);
        isTrueArgument("hexString has 24 characters", s.length() == 24);

        byte[] b = new byte[OBJECT_ID_LENGTH];
        for (int i = 0; i < b.length; i++) {
            int pos = i << 1;
            char c1 = s.charAt(pos);
            char c2 = s.charAt(pos + 1);
            b[i] = (byte) ((hexCharToInt(c1) << 4) + hexCharToInt(c2));
        }
        return b;
    }

    private static int hexCharToInt(final char c) {
        if (c >= '0' && c <= '9') {
            return c - 48;
        } else if (c >= 'a' && c <= 'f') {
            return c - 87;
        } else if (c >= 'A' && c <= 'F') {
            return c - 55;
        }
        throw new IllegalArgumentException("invalid hexadecimal character: [" + c + "]");
    }

    private static int dateToTimestampSeconds(final Date time) {
        return (int) (time.getTime() / 1000);
    }
}
