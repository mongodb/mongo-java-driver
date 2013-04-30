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
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class BSONDecoderTest {
    private BSONDecoder bsonDecoder;

    @Before
    public void setUp() {
        bsonDecoder = new BasicBSONDecoder();
    }

    private void testDecoder(final byte[] bytes, final BSONObject expectedObject) {
        final BSONObject receivedObject = bsonDecoder.readObject(bytes);
        assertEquals(expectedObject, receivedObject);
    }

    @Test
    public void testDecodingNumbers() {
        testDecoder(new byte[]{
                45, 0, 0, 0, 16, 105, 49, 0, -12,
                -1, -1, -1, 16, 105, 50, 0, 0, 0,
                0, -128, 18, 105, 51, 0, -1, -1, -1,
                -1, -1, -1, -1, 127, 18, 105, 52, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0
        },
                new BasicBSONObject("i1", -12)
                        .append("i2", Integer.MIN_VALUE)
                        .append("i3", Long.MAX_VALUE)
                        .append("i4", 0)
        );
    }

    @Test
    public void testDecodingBoolean() {
        testDecoder(
                new byte[]{15, 0, 0, 0, 8, 98, 49, 0, 1, 8, 98, 50, 0, 0, 0},
                new BasicBSONObject("b1", true).append("b2", false)
        );
    }

    @Test
    public void testDecodingString() {
        testDecoder(new byte[]{82, 0, 0, 0, 2, 115, 49, 0, 1, 0, 0, 0, 0, 2, 115, 50,
                0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 2, 115, 51, 0, 23,
                0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35,
                36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 2, 115, 53, 0, 15, 0,
                0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0,
                0},
                new BasicBSONObject("s1", "")
                        .append("s2", "danke")
                        .append("s3", ",+\\\"<>;[]{}@#$%^&*()+_")
                        .append("s5", "a\u00e9\u3042\u0430\u0432\u0431\u0434")
        );
    }

    @Test
    public void testDecodingNull() {
        testDecoder(
                new byte[]{13, 0, 0, 0, 10, 110, 49, 0, 10, 110, 50, 0, 0},
                new BasicBSONObject("n1", null).append("n2", null)
        );
    }

    @Test
    public void testDecodingObjectId() {
        testDecoder(new byte[]{
                22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45,
                51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0},
                new BasicBSONObject("_id", new ObjectId("50d3332018c6a1d8d1662b61")));
    }

    @Test
    public void testDecodingCode() {
        testDecoder(new byte[]{
                53, 0, 0, 0, 13, 106, 115, 49, 0, 10, 0, 0,
                0, 118, 97, 114, 32, 105, 32, 61, 32, 48,
                0, 15, 106, 115, 50, 0, 24, 0, 0, 0, 4, 0,
                0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16,
                120, 0, 1, 0, 0, 0, 0, 0},
                new BasicBSONObject("js1", new Code("var i = 0"))
                        .append("js2", new CodeWScope("i++", new BasicBSONObject("x", 1)))
        );
    }

    @Test
    public void testDecodingHierarchyOfJavascriptWithScope() {
        testDecoder(new byte[]{
                55, 0, 0, 0, 15, 106, 115, 0, 46, 0, 0,
                0, 4, 0, 0, 0, 105, 43, 43, 0, 34, 0, 0,
                0, 15, 110, 106, 115, 0, 24, 0, 0, 0, 4,
                0, 0, 0, 106, 43, 43, 0, 12, 0, 0, 0, 16,
                106, 0, 0, 0, 0, 0, 0, 0, 0},
                new BasicBSONObject("js", new CodeWScope("i++", new BasicBSONObject("njs", new CodeWScope("j++", new BasicBSONObject("j", 0)))))
        );
    }

    @Test
    public void testDecodingTimestamp() {
        testDecoder(
                new byte[]{17, 0, 0, 0, 17, 116, 49, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0},
                new BasicBSONObject("t1", new BSONTimestamp(123999401, 44332))
        );
    }

    @Test
    public void testDecodingMinMaxKeys() {
        testDecoder(
                new byte[]{17, 0, 0, 0, 127, 107, 49, 0, -1, 107, 50, 0, 127, 107, 51, 0, 0},
                new BasicBSONObject("k1", new MaxKey()).append("k2", new MinKey()).append("k3", new MaxKey())
        );
    }

    @Test
    public void testDecodingArray() {
        testDecoder(new byte[]{
                31, 0, 0, 0, 4, 97, 49, 0, 5, 0, 0, 0,
                0, 4, 97, 50, 0, 13, 0, 0, 0, 4, 48, 0, 5,
                0, 0, 0, 0, 0, 0},
                new BasicBSONObject("a1", Collections.emptyList())
                        .append("a2", Collections.singletonList(Collections.emptyList()))
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testDecodingNestedArraysWithInts() {
        testDecoder(new byte[]{
                35, 0, 0, 0, 4, 97, 0, 27, 0,
                0, 0, 4, 48, 0, 19, 0, 0, 0,
                16, 48, 0, 1, 0, 0, 0, 16, 49,
                0, 2, 0, 0, 0, 0, 0, 0},
                new BasicBSONObject("a", Arrays.asList(Arrays.asList(1, 2))));
    }

    @Test
    public void testDecodingNestedObjectsInArray() {
        testDecoder(new byte[]{
                39, 0, 0, 0, 4, 97, 0, 31, 0, 0, 0,
                3, 48, 0, 10, 0, 0, 0, 8, 98, 49, 0,
                1, 0, 3, 49, 0, 10, 0, 0, 0, 8, 98,
                50, 0, 0, 0, 0, 0},
                new BasicBSONObject("a", Arrays.asList(
                        new BasicBSONObject("b1", true),
                        new BasicBSONObject("b2", false))
                ));
    }

    @Test
    public void testDecodingNestedObject() {
        testDecoder(new byte[]{
                39, 0, 0, 0, 3, 97, 0, 31, 0, 0,
                0, 3, 100, 49, 0, 9, 0, 0, 0, 8,
                98, 0, 1, 0, 3, 100, 50, 0, 9, 0,
                0, 0, 8, 98, 0, 0, 0, 0, 0
        }, new BasicBSONObject("a",
                new BasicBSONObject("d1", new BasicBSONObject("b", true))
                        .append("d2", new BasicBSONObject("b", false))));
    }
}
