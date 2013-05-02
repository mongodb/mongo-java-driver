/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.bson;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;

public class BSONEncoderTest {

    private BSONEncoder bsonEncoder;

    @Before
    public void setUp() {
        bsonEncoder = new BasicBSONEncoder();
    }

    private void testEncoder(final BSONObject document, final byte[] expectedBytes) {
        final byte[] receivedBytes = bsonEncoder.encode(document);
        assertArrayEquals(expectedBytes, receivedBytes);
    }

    @Test
    public void testEncodingNumbers() {
        testEncoder(new BasicBSONObject("i1", -12)
                .append("i2", Integer.MIN_VALUE)
                .append("i3", Long.MAX_VALUE)
                .append("i4", 0), new byte[]{
                41, 0, 0, 0, 16, 105, 49, 0, -12,
                -1, -1, -1, 16, 105, 50, 0, 0, 0,
                0, -128, 18, 105, 51, 0, -1, -1, -1,
                -1, -1, -1, -1, 127, 16, 105, 52, 0,
                0, 0, 0, 0, 0
        }
        );
    }

    @Test
    public void testEncodingBoolean() {
        testEncoder(
                new BasicBSONObject("b1", true).append("b2", false), new byte[]{15, 0, 0, 0, 8, 98, 49, 0, 1, 8, 98, 50, 0, 0, 0}
        );
    }

    @Test
    public void testEncodingString() {
        testEncoder(new BasicBSONObject("s1", "")
                .append("s2", "danke")
                .append("s3", ",+\\\"<>;[]{}@#$%^&*()+_")
                .append("s5", "a\u00e9\u3042\u0430\u0432\u0431\u0434"), new byte[]{82, 0, 0, 0, 2, 115, 49, 0, 1, 0, 0, 0, 0, 2, 115, 50,
                0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 2, 115, 51, 0, 23,
                0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35,
                36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 2, 115, 53, 0, 15, 0,
                0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0,
                0}
        );
    }

    @Test
    public void testEncodingNull() {
        testEncoder(
                new BasicBSONObject("n1", null).append("n2", null), new byte[]{13, 0, 0, 0, 10, 110, 49, 0, 10, 110, 50, 0, 0}
        );
    }

    @Test
    public void testEncodingObjectId() {
        testEncoder(new BasicBSONObject("_id", new ObjectId("50d3332018c6a1d8d1662b61")), new byte[]{
                22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45,
                51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0}
        );
    }

//    @Test
//    public void testEncodingCode() {
//        testEncoder(new BasicBSONObject("js1", new Code("var i = 0"))
//                .append("js2", new CodeWScope("i++", new BasicBSONObject("x", 1))), new byte[]{
//                53, 0, 0, 0, 13, 106, 115, 49, 0, 10, 0, 0,
//                0, 118, 97, 114, 32, 105, 32, 61, 32, 48,
//                0, 15, 106, 115, 50, 0, 24, 0, 0, 0, 4, 0,
//                0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16,
//                120, 0, 1, 0, 0, 0, 0, 0}
//        );
//    }
//
//    @Test
//    public void testEncodingHierarchyOfJavascriptWithScope() {
//        testEncoder(new BasicBSONObject("js", new CodeWScope("i++", new BasicBSONObject("njs", new CodeWScope("j++", new BasicBSONObject("j", 0))))), new byte[]{
//                55, 0, 0, 0, 15, 106, 115, 0, 46, 0, 0,
//                0, 4, 0, 0, 0, 105, 43, 43, 0, 34, 0, 0,
//                0, 15, 110, 106, 115, 0, 24, 0, 0, 0, 4,
//                0, 0, 0, 106, 43, 43, 0, 12, 0, 0, 0, 16,
//                106, 0, 0, 0, 0, 0, 0, 0, 0}
//        );
//    }

    @Test
    public void testEncodingTimestamp() {
        testEncoder(
                new BasicBSONObject("t1", new BSONTimestamp(123999401, 44332)), new byte[]{17, 0, 0, 0, 17, 116, 49, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0}
        );
    }

    @Test
    public void testEncodingMinMaxKeys() {
        testEncoder(
                new BasicBSONObject("k1", new MaxKey()).append("k2", new MinKey()).append("k3", new MaxKey()), new byte[]{17, 0, 0, 0, 127, 107, 49, 0, -1, 107, 50, 0, 127, 107, 51, 0, 0}
        );
    }

    @Test
    public void testEncodingList() {
        testEncoder(new BasicBSONObject("a1", Collections.emptyList())
                .append("a2", Collections.singletonList(Collections.emptyList())), new byte[]{
                31, 0, 0, 0, 4, 97, 49, 0, 5, 0, 0, 0,
                0, 4, 97, 50, 0, 13, 0, 0, 0, 4, 48, 0, 5,
                0, 0, 0, 0, 0, 0}
        );
    }

    @Test
    public void testEncodingArray() {
        testEncoder(
                new BasicBSONObject("a", new int[]{3, 5}),
                new byte[]{27, 0, 0, 0, 4, 97, 0, 19, 0, 0, 0, 16, 48, 0, 3, 0, 0, 0, 16, 49, 0, 5, 0, 0, 0, 0, 0}
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testEncodingNestedListsWithInts() {
        testEncoder(new BasicBSONObject("a", Arrays.asList(Arrays.asList(1, 2))), new byte[]{
                35, 0, 0, 0, 4, 97, 0, 27, 0,
                0, 0, 4, 48, 0, 19, 0, 0, 0,
                16, 48, 0, 1, 0, 0, 0, 16, 49,
                0, 2, 0, 0, 0, 0, 0, 0}
        );
    }

    @Test
    public void testEncodingNestedObjectsInArray() {
        testEncoder(new BasicBSONObject("a", Arrays.asList(
                new BasicBSONObject("b1", true),
                new BasicBSONObject("b2", false))
        ), new byte[]{
                39, 0, 0, 0, 4, 97, 0, 31, 0, 0, 0,
                3, 48, 0, 10, 0, 0, 0, 8, 98, 49, 0,
                1, 0, 3, 49, 0, 10, 0, 0, 0, 8, 98,
                50, 0, 0, 0, 0, 0}
        );
    }

    @Test
    public void testEncodingNestedObject() {
        testEncoder(new BasicBSONObject("a",
                new BasicBSONObject("d1", new BasicBSONObject("b", true))
                        .append("d2", new BasicBSONObject("b", false))), new byte[]{
                39, 0, 0, 0, 3, 97, 0, 31, 0, 0,
                0, 3, 100, 49, 0, 9, 0, 0, 0, 8,
                98, 0, 1, 0, 3, 100, 50, 0, 9, 0,
                0, 0, 8, 98, 0, 0, 0, 0, 0
        });
    }

    @Test
    public void testEncodingBinary() {
        final byte[] bytes = {
                29, 0, 0, 0, 5, 98, 49, 0, 3, 0, 0,
                0, 1, 115, 116, 114, 5, 98, 50, 0,
                3, 0, 0, 0, 0, 102, 111, 111, 0
        };

        testEncoder(
                new BasicBSONObject("b1", new Binary((byte) 0x01, new byte[]{115, 116, 114}))
                        .append("b2", new byte[]{102, 111, 111}),
                bytes
        );
    }

    @Test
    public void testEncodingDateTime() {
        testEncoder(
                new BasicBSONObject("dt", new Date(582163200)),
                new byte[]{
                        17, 0, 0, 0, 9, 100, 116, 0, 0, 27, -77, 34, 0, 0, 0, 0, 0
                });
    }

    @Test
    public void testEncodingDouble() {
        testEncoder(new BasicBSONObject("d1", -1.01)
                .append("d2", Float.MIN_VALUE)
                .append("d3", Double.MAX_VALUE)
                .append("d4", 0.0),
                new byte[]{
                        53, 0, 0, 0, 1, 100, 49, 0, 41, 92, -113,
                        -62, -11, 40, -16, -65, 1, 100, 50, 0, 0,
                        0, 0, 0, 0, 0, -96, 54, 1, 100, 51, 0, -1,
                        -1, -1, -1, -1, -1, -17, 127, 1, 100, 52,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                });
    }

    @Test
    public void testEncodingRegularExpression() {
        testEncoder(new BasicBSONObject("rx", Pattern.compile("[a]*", Pattern.CASE_INSENSITIVE)),
                new byte[]{
                        16, 0, 0, 0, 11, 114, 120, 0, 91, 97, 93, 42, 0, 105, 0, 0
                });
    }

    @Test
    public void testEncodingSymbol() {
        testEncoder(new BasicBSONObject("s", new Symbol("c")),
                new byte[]{
                        14, 0, 0, 0, 14, 115, 0, 2, 0, 0, 0, 99, 0, 0
                });
    }

    @Test
    public void testEncodingMap() {
        final Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("a", 1);
        testEncoder(new BasicBSONObject("m", map),
                new byte[]{
                        20, 0, 0, 0, 3, 109, 0, 12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0, 0
                });
    }

}
