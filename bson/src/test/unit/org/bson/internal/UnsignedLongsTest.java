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

package org.bson.internal;

import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnsignedLongsTest {

    @Test
    public void testCompare() {
        // max value
        assertTrue(UnsignedLongs.compare(0, 0xffffffffffffffffL) < 0);
        assertTrue(UnsignedLongs.compare(0xffffffffffffffffL, 0) > 0);

        // both with high bit set
        assertTrue(UnsignedLongs.compare(0xff1a618b7f65ea12L, 0xffffffffffffffffL) < 0);
        assertTrue(UnsignedLongs.compare(0xffffffffffffffffL, 0xff1a618b7f65ea12L) > 0);

        // one with high bit set
        assertTrue(UnsignedLongs.compare(0x5a4316b8c153ac4dL, 0xff1a618b7f65ea12L) < 0);
        assertTrue(UnsignedLongs.compare(0xff1a618b7f65ea12L, 0x5a4316b8c153ac4dL) > 0);

        // neither with high bit set
        assertTrue(UnsignedLongs.compare(0x5a4316b8c153ac4dL, 0x6cf78a4b139a4e2aL) < 0);
        assertTrue(UnsignedLongs.compare(0x6cf78a4b139a4e2aL, 0x5a4316b8c153ac4dL) > 0);

        // same value
        assertTrue(UnsignedLongs.compare(0xff1a618b7f65ea12L, 0xff1a618b7f65ea12L) == 0);
    }

    @Test
    public void testParseLong() {
        assertEquals(0xffffffffffffffffL, UnsignedLongs.parse("18446744073709551615"));
        assertEquals(0x7fffffffffffffffL, UnsignedLongs.parse("9223372036854775807"));
        assertEquals(0xff1a618b7f65ea12L, UnsignedLongs.parse("18382112080831834642"));
        assertEquals(0x5a4316b8c153ac4dL, UnsignedLongs.parse("6504067269626408013"));
        assertEquals(0x6cf78a4b139a4e2aL, UnsignedLongs.parse("7851896530399809066"));
    }

    @Test
    public void testToString() {
        String[] tests = {
                "ffffffffffffffff",
                "7fffffffffffffff",
                "ff1a618b7f65ea12",
                "5a4316b8c153ac4d",
                "6cf78a4b139a4e2a"
        };
        for (String x : tests) {
            BigInteger xValue = new BigInteger(x, 16);
            long xLong = xValue.longValue(); // signed
            assertEquals(xValue.toString(10), UnsignedLongs.toString(xLong));
        }
    }

}
