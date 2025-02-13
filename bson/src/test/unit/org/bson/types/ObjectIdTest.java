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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ObjectIdTest {

    /** Calls the base method of ByteBuffer.position(int) since the override is not available in jdk8. */
    private static ByteBuffer setPosition(final ByteBuffer buf, final int pos) {
        ((Buffer) buf).position(pos);
        return buf;
    }

    /**
     * MethodSource for valid ByteBuffers that can hold an ObjectID
     */
    public static List<ByteBuffer> validOutputBuffers() {
        List<ByteBuffer> result = new ArrayList<>();
        result.add(ByteBuffer.allocate(12));
        result.add(ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN));
        result.add(ByteBuffer.allocate(24).put(new byte[12]));
        result.add(ByteBuffer.allocateDirect(12));
        result.add(ByteBuffer.allocateDirect(12).order(ByteOrder.LITTLE_ENDIAN));
        return result;
    }

    @MethodSource("validOutputBuffers")
    @ParameterizedTest
    public void testToBytes(final ByteBuffer output) {
        int originalPosition = output.position();
        ByteOrder originalOrder = output.order();
        byte[] expectedBytes = {81, 6, -4, -102, -68, -126, 55, 85, -127, 54, -46, -119};
        byte[] result = new byte[12];
        ObjectId objectId = new ObjectId(expectedBytes);

        assertArrayEquals(expectedBytes, objectId.toByteArray());

        objectId.putToByteBuffer(output);
        ((Buffer) output).position(output.position() - 12);
        output.get(result); // read last 12 bytes leaving position intact

        assertArrayEquals(expectedBytes, result);
        assertEquals(originalPosition + 12, output.position());
        assertEquals(originalOrder, output.order());
    }

    @Test
    public void testFromBytes() {

        try {
            new ObjectId((byte[]) null);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("bytes can not be null", e.getMessage());
        }

        try {
            new ObjectId(new byte[11]);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("state should be: bytes has length of 12", e.getMessage());
        }

        try {
            new ObjectId(new byte[13]);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals("state should be: bytes has length of 12", e.getMessage());
        }

        byte[] bytes = {81, 6, -4, -102, -68, -126, 55, 85, -127, 54, -46, -119};

        ObjectId objectId1 = new ObjectId(bytes);
        assertEquals(0x5106FC9A, objectId1.getTimestamp());

        ObjectId objectId2 = new ObjectId(ByteBuffer.wrap(bytes));
        assertEquals(0x5106FC9A, objectId2.getTimestamp());
    }

    @Test
    public void testBytesRoundtrip() {
        ObjectId expected = new ObjectId();
        ObjectId actual = new ObjectId(expected.toByteArray());
        assertEquals(expected, actual);

        byte[] b = new byte[12];
        Random r = new Random(17);
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) (r.nextInt());
        }
        expected = new ObjectId(b);
        assertEquals(expected, new ObjectId(expected.toByteArray()));
        assertEquals("41d91c58988b09375cc1fe9f", expected.toString());
    }

    @Test
    public void testGetSmallestWithDate() {
        Date date = new Date(1588467737760L);
        byte[] expectedBytes = {94, -82, 24, 25, 0, 0, 0, 0, 0, 0, 0, 0};
        ObjectId objectId = ObjectId.getSmallestWithDate(date);
        assertArrayEquals(expectedBytes, objectId.toByteArray());
        assertEquals(date.getTime() / 1000 * 1000, objectId.getDate().getTime());
        assertEquals(-1, objectId.compareTo(new ObjectId(date)));
    }

    @Test
    public void testGetTimeZero() {
        assertEquals(0L, new ObjectId(0, 0).getDate().getTime());
    }

    @Test
    public void testGetTimeMaxSignedInt() {
        assertEquals(0x7FFFFFFFL * 1000, new ObjectId(0x7FFFFFFF, 0).getDate().getTime());
    }

    @Test
    public void testGetTimeMaxSignedIntPlusOne() {
        assertEquals(0x80000000L * 1000, new ObjectId(0x80000000, 0).getDate().getTime());
    }

    @Test
    public void testGetTimeMaxInt() {
        assertEquals(0xFFFFFFFFL * 1000, new ObjectId(0xFFFFFFFF, 0).getDate().getTime());
    }

    @Test
    public void testTime() {
        long a = System.currentTimeMillis();
        long b = (new ObjectId()).getDate().getTime();
        assertTrue(Math.abs(b - a) < 3000);
    }

    @Test
    public void testDateConstructor() {
        assertEquals(new Date().getTime() / 1000, new ObjectId(new Date()).getDate().getTime() / 1000);
        assertNotEquals(new ObjectId(new Date(1_000)), new ObjectId(new Date(1_000)));
        assertEquals("00000001", new ObjectId(new Date(1_000)).toHexString().substring(0, 8));
    }

    @Test
    public void testDateConstructorWithCounter() {
        assertEquals(new ObjectId(new Date(1_000), 1), new ObjectId(new Date(1_000), 1));
        assertEquals("00000001", new ObjectId(new Date(1_000), 1).toHexString().substring(0, 8));
        assertThrows(NullPointerException.class, () -> new ObjectId(null, Integer.MAX_VALUE));
        assertThrows(IllegalArgumentException.class, () -> new ObjectId(new Date(1_000), Integer.MAX_VALUE));
    }

    @Test
    public void testTimestampConstructor() {
        assertEquals(1_000, new ObjectId(1_000, 1).getTimestamp());
        assertEquals(new ObjectId(1_000, 1), new ObjectId(1_000, 1));
        assertEquals("7fffffff", new ObjectId(Integer.MAX_VALUE, 1).toHexString().substring(0, 8));
        assertThrows(IllegalArgumentException.class, () -> new ObjectId(Integer.MAX_VALUE, Integer.MAX_VALUE));
    }

    /**
     * MethodSource for valid ByteBuffers containing an ObjectID at the current position.
     */
    public static List<ByteBuffer> validInputBuffers() {
        byte[] data = new byte[12];
        for (byte i = 0; i < data.length; ++i) {
            data[i] = i;
        }

        List<ByteBuffer> result = new ArrayList<>();
        result.add(ByteBuffer.wrap(data));
        result.add(ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN));
        result.add(setPosition(ByteBuffer.allocateDirect(data.length).put(data), 0));
        result.add(setPosition(ByteBuffer.allocateDirect(data.length).put(data).order(ByteOrder.LITTLE_ENDIAN), 0));
        result.add(setPosition(ByteBuffer.allocate(2 * data.length).put(data), 0));
        result.add(setPosition(ByteBuffer.allocate(2 * data.length).put(new byte[12]).put(data), 12));
        return result;
    }

    @ParameterizedTest
    @MethodSource(value = "validInputBuffers")
    public void testByteBufferConstructor(final ByteBuffer input) {
        ByteOrder order = input.order();
        int position = input.position();

        byte[] result = new ObjectId(input).toByteArray();

        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, result);
        assertEquals(order, input.order());
        assertEquals(position + 12, input.position());
    }

    @Test
    public void testInvalidByteBufferConstructor() {
        assertThrows(IllegalArgumentException.class, () -> new ObjectId((ByteBuffer) null));
        assertThrows(IllegalArgumentException.class, () -> new ObjectId(ByteBuffer.allocate(11)));
    }

    @Test
    public void testHexStringConstructor() {
        ObjectId id = new ObjectId();
        assertEquals(id, new ObjectId(id.toHexString()));
        assertEquals(id, new ObjectId(id.toHexString().toUpperCase(Locale.US)));
        assertThrows(IllegalArgumentException.class, () -> new ObjectId((String) null));
        assertThrows(IllegalArgumentException.class, () -> new ObjectId(id.toHexString().substring(0, 23)));
        assertThrows(IllegalArgumentException.class, () -> new ObjectId(id.toHexString().substring(0, 23) + '%'));
    }

    @Test
    public void testCompareTo() {
        Date dateOne = new Date();
        Date dateTwo = new Date(dateOne.getTime() + 10000);
        ObjectId first = new ObjectId(dateOne, 0);
        ObjectId second = new ObjectId(dateOne, 1);
        ObjectId third = new ObjectId(dateTwo, 0);
        assertEquals(0, first.compareTo(first));
        assertEquals(-1, first.compareTo(second));
        assertEquals(-1, first.compareTo(third));
        assertEquals(1, second.compareTo(first));
        assertEquals(1, third.compareTo(first));
        assertThrows(NullPointerException.class, () -> first.compareTo(null));
    }

    @Test
    public void testEquals() {
        Date dateOne = new Date();
        Date dateTwo = new Date(dateOne.getTime() + 10000);
        ObjectId first = new ObjectId(dateOne, 0);
        ObjectId second = new ObjectId(dateOne, 1);
        ObjectId third = new ObjectId(dateTwo, 0);
        ObjectId fourth = new ObjectId(first.toByteArray());
        assertEquals(first, first);
        assertEquals(first, fourth);
        assertNotEquals(first, second);
        assertNotEquals(first, third);
        assertNotEquals(second, third);
        assertFalse(first.equals(null));
    }

    @Test
    public void testToHexString() {
        assertEquals("000000000000000000000000", new ObjectId(new byte[12]).toHexString());
        assertEquals("7fffffff007fff7fff007fff", new ObjectId(new byte[]{127, -1, -1, -1, 0, 127, -1, 127, -1, 0, 127, -1}).toHexString());
    }

    private Date getDate(final String s) throws ParseException {
        return new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss Z").parse(s);
    }

    @Test
    public void testTimeZero() throws ParseException {
        assertEquals(getDate("01-Jan-1970 00:00:00 -0000"), new ObjectId(0, 0).getDate());
    }

    @Test
    public void testTimeMaxSignedInt() throws ParseException {
        assertEquals(getDate("19-Jan-2038 03:14:07 -0000"), new ObjectId(0x7FFFFFFF, 0).getDate());
    }

    @Test
    public void testTimeMaxSignedIntPlusOne() throws ParseException {
        assertEquals(getDate("19-Jan-2038 03:14:08 -0000"), new ObjectId(0x80000000, 0).getDate());
    }

    @Test
    public void testTimeMaxInt() throws ParseException {
        assertEquals(getDate("07-Feb-2106 06:28:15 -0000"), new ObjectId(0xFFFFFFFF, 0).getDate());
    }

    @Test
    public void testObjectSerialization() throws IOException, ClassNotFoundException {
        // given
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        ObjectId objectId = new ObjectId("5f8f4fcf27516f05e7eae5be");

        // when
        oos.writeObject(objectId);

        // then
        assertTrue(baos.toString().contains("org.bson.types.ObjectId$SerializationProxy"));
        assertArrayEquals(new byte[] {-84, -19, 0, 5, 115, 114, 0, 42, 111, 114, 103, 46, 98, 115, 111, 110, 46, 116, 121, 112, 101, 115,
                        46, 79, 98, 106, 101, 99, 116, 73, 100, 36, 83, 101, 114, 105, 97, 108, 105, 122, 97, 116, 105, 111, 110, 80, 114,
                        111, 120, 121, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 1, 91, 0, 5, 98, 121, 116, 101, 115, 116, 0, 2, 91, 66, 120, 112, 117,
                        114, 0, 2, 91, 66, -84, -13, 23, -8, 6, 8, 84, -32, 2, 0, 0, 120, 112, 0, 0, 0, 12, 95, -113, 79, -49, 39, 81, 111,
                        5, -25, -22, -27, -66}, baos.toByteArray());

        // when
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        ObjectId deserializedObjectId = (ObjectId) ois.readObject();

        // then
        assertEquals(objectId, deserializedObjectId);
    }
}
