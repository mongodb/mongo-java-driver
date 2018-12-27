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

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class ObjectIdTest {
    @Test
    public void testToBytes() {
        ObjectId objectId = new ObjectId(0x5106FC9A, 0x00BC8237, (short) 0x5581, 0x0036D289);
        byte[] expectedBytes = new byte[]{81, 6, -4, -102, -68, -126, 55, 85, -127, 54, -46, -119};

        assertArrayEquals(expectedBytes, objectId.toByteArray());

        ByteBuffer buffer = ByteBuffer.allocate(12);
        objectId.putToByteBuffer(buffer);
        assertArrayEquals(expectedBytes, buffer.array());
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

        byte[] bytes = new byte[]{81, 6, -4, -102, -68, -126, 55, 85, -127, 54, -46, -119};

        ObjectId objectId1 = new ObjectId(bytes);
        assertEquals(0x5106FC9A, objectId1.getTimestamp());
        assertEquals(0x00BC8237, objectId1.getMachineIdentifier());
        assertEquals((short) 0x5581, objectId1.getProcessIdentifier());
        assertEquals(0x0036D289, objectId1.getCounter());

        ObjectId objectId2 = new ObjectId(ByteBuffer.wrap(bytes));
        assertEquals(0x5106FC9A, objectId2.getTimestamp());
        assertEquals(0x00BC8237, objectId2.getMachineIdentifier());
        assertEquals((short) 0x5581, objectId2.getProcessIdentifier());
        assertEquals(0x0036D289, objectId2.getCounter());
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
    public void testGetTimeZero() {
        assertEquals(0L, new ObjectId(0, 0).getTime());
    }

    @Test
    public void testGetTimeMaxSignedInt() {
        assertEquals(0x7FFFFFFFL * 1000, new ObjectId(0x7FFFFFFF, 0).getTime());
    }

    @Test
    public void testGetTimeMaxSignedIntPlusOne() {
        assertEquals(0x80000000L * 1000, new ObjectId(0x80000000, 0).getTime());
    }

    @Test
    public void testGetTimeMaxInt() {
        assertEquals(0xFFFFFFFFL * 1000, new ObjectId(0xFFFFFFFF, 0).getTime());
    }

    @Test
    public void testTime() {
        long a = System.currentTimeMillis();
        long b = (new ObjectId()).getDate().getTime();
        assertTrue(Math.abs(b - a) < 3000);
    }

    @Test
    public void testDateCons() {
        assertEquals(new Date().getTime() / 1000, new ObjectId(new Date()).getDate().getTime() / 1000);
    }

    @Test
    public void testMachineIdentifier() {
        assertTrue(ObjectId.getGeneratedMachineIdentifier() > 0);
        assertEquals(0, ObjectId.getGeneratedMachineIdentifier() & 0xff000000);

        assertEquals(5, new ObjectId(0, 5, (short) 0, 0).getMachineIdentifier());
        assertEquals(0x00ffffff, new ObjectId(0, 0x00ffffff, (short) 0, 0).getMachineIdentifier());
        assertEquals(ObjectId.getGeneratedMachineIdentifier(), new ObjectId().getMachineIdentifier());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfMachineIdentifierIsTooLarge() {
        new ObjectId(0, 0x00ffffff + 1, (short) 0, 0);
    }

    @Test
    public void testProcessIdentifier() {
        assertEquals(5, new ObjectId(0, 0, (short) 5, 0).getProcessIdentifier());
        assertEquals(ObjectId.getGeneratedProcessIdentifier(), new ObjectId().getProcessIdentifier());
    }

    @Test
    public void testCounter() {
        assertEquals(new ObjectId().getCounter() + 1, new ObjectId().getCounter());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfCounterIsTooLarge() {
        new ObjectId(0, 0, (short) 0, 0x00ffffff + 1);
    }

    @Test
    public void testHexStringConstructor() {
        ObjectId id = new ObjectId();
        assertEquals(id, new ObjectId(id.toHexString()));
    }

    @Test
    public void testCompareTo() {
        assertEquals(-1, new ObjectId(0, 0, (short) 0, 0).compareTo(new ObjectId(1, 0, (short) 0, 0)));
        assertEquals(-1, new ObjectId(0, 0, (short) 0, 0).compareTo(new ObjectId(0, 1, (short) 0, 0)));
        assertEquals(-1, new ObjectId(0, 0, (short) 0, 0).compareTo(new ObjectId(0, 0, (short) 1, 0)));
        assertEquals(-1, new ObjectId(0, 0, (short) 1, 0).compareTo(new ObjectId(0, 0, (short) -1, 0)));
        assertEquals(-1, new ObjectId(0, 0, (short) 0, 0).compareTo(new ObjectId(0, 0, (short) 0, 1)));
        assertEquals(0, new ObjectId(0, 0, (short) 0, 0).compareTo(new ObjectId(0, 0, (short) 0, 0)));
        assertEquals(1, new ObjectId(1, 0, (short) 0, 0).compareTo(new ObjectId(0, 0, (short) 0, 0)));
        assertEquals(1, new ObjectId(0, 1, (short) 0, 0).compareTo(new ObjectId(0, 0, (short) 0, 0)));
        assertEquals(1, new ObjectId(0, 0, (short) 1, 0).compareTo(new ObjectId(0, 0, (short) 0, 0)));
        assertEquals(1, new ObjectId(0, 0, (short) -1, 0).compareTo(new ObjectId(0, 0, (short) 1, 0)));
        assertEquals(1, new ObjectId(0, 0, (short) 0, 1).compareTo(new ObjectId(0, 0, (short) 0, 0)));
    }

    @Test
    public void testToHexString() {
        assertEquals("000000000000000000000000", new ObjectId(0, 0, (short) 0, 0).toHexString());
        assertEquals("7fffffff007fff7fff007fff",
                new ObjectId(Integer.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE, Short.MAX_VALUE).toHexString());
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

    @SuppressWarnings("deprecation")
    @Test
    public void testDeprecatedMethods() {

        ObjectId id = new ObjectId();
        assertEquals(id.getTimestamp(), id.getTimeSecond());
        assertEquals(id.getDate().getTime(), id.getTime());
        assertEquals(id.toHexString(), id.toStringMongod());
        assertArrayEquals(new byte[]{0x12, 0x34, 0x56, 0x78, 0x43, 0x21, 0xffffff87, 0x65, 0x74, 0xffffff92, 0xffffff87, 0x56},
                new ObjectId(0x12345678, 0x43218765, 0x74928756).toByteArray());
    }

    // Got these values from 2.12.0 driver.  This test is ensuring that we properly round-trip old and new format ObjectIds.
    @Test
    public void testCreateFromLegacy() {
        assertArrayEquals(new byte[]{82, 23, -82, -78, -80, -58, -95, -92, -75, -38, 118, -16},
                ObjectId.createFromLegacyFormat(1377283762, -1329159772, -1243973904).toByteArray());
    }
}
