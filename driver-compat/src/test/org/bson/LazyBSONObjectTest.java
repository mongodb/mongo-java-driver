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

import org.bson.types.Binary;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.hasItems;

public class LazyBSONObjectTest {
    private byte[] getBytes(final BSONObject object) {
        return BSON.encode(object);
    }

    @Test
    public void testObjectWith2Fields() {
        final byte[] bytes = getBytes(new BasicBSONObject("a", 1).append("b", true));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(2, lazy.keySet().size());
        assertThat(lazy.keySet(), hasItems("a", "b"));
        assertEquals(1, lazy.get("a"));
        assertEquals(true, lazy.get("b"));
    }

    @Test
    public void testObjectWithBinaryOfStandardType() {
        final byte[] bytes = getBytes(new BasicBSONObject("b", new Binary(new byte[]{13, 12})));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(1, lazy.keySet().size());
        assertThat(lazy.get("b"), instanceOf(byte[].class));
        assertArrayEquals(new byte[]{13, 12}, (byte[]) lazy.get("b"));
    }

    @Test
    public void testEmptyObject() {
        final byte[] bytes = getBytes(new BasicBSONObject());
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertTrue(lazy.keySet().isEmpty());
    }


    @Test
    public void testContainsField() {
        final byte[] bytes = getBytes(new BasicBSONObject("a", 1).append("b", true));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertTrue(lazy.containsField("a"));
        assertFalse(lazy.containsField("z"));
    }

    @Test
    public void testGettingNonexistentField() {
        final byte[] bytes = getBytes(new BasicBSONObject("a", 1).append("b", true));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertNull(lazy.get("z"));
    }

    @Test(expected = Exception.class)
    public void testKeySetModification() {
        final byte[] bytes = getBytes(new BasicBSONObject("a", 1).append("b", true));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        lazy.keySet().add("c");
    }

    @Test
    public void testPipingObjectIntoStream() throws IOException {
        final byte[] bytes = getBytes(new BasicBSONObject("a", 1).append("b", true));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        lazy.pipe(baos);
        assertArrayEquals(bytes, baos.toByteArray());
    }

    @Test
    public void testEntrySet() {
        final byte[] bytes = getBytes(new BasicBSONObject("a", 1).append("b", true));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        final Set<Map.Entry<String, Object>> entrySet = lazy.entrySet();
        assertEquals(2, entrySet.size());
    }

    @Test
    public void testObjectWithNestedObject() throws IOException {
        final byte[] bytes = getBytes(new BasicBSONObject("o", new BasicBSONObject("a", 1)));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(1, lazy.keySet().size());
        assertThat(lazy.get("o"), instanceOf(LazyBSONObject.class));
        final LazyBSONObject nested = (LazyBSONObject) lazy.get("o");
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        nested.pipe(baos);
        assertArrayEquals(new byte[]{12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0}, baos.toByteArray());
        assertEquals(1, nested.get("a"));
    }

    @Test
    public void testObjectWithNestedObjectFollowedByAField() throws IOException {
        final byte[] bytes = getBytes(new BasicBSONObject("o", new BasicBSONObject("z", 1)).append("b", 2));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(2, lazy.keySet().size());
        assertThat(lazy.get("o"), instanceOf(LazyBSONObject.class));
        assertEquals(2, lazy.get("b"));
    }

    @Test
    public void testObjectWithReqularExpression() {
        final byte[] bytes = new byte[]{16, 0, 0, 0, 11, 114, 120, 0, 91, 97, 93, 42, 0, 105, 0, 0};
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertThat(lazy.get("rx"), instanceOf(Pattern.class));
        final Pattern pattern = (Pattern) lazy.get("rx");
        assertEquals(Pattern.CASE_INSENSITIVE, pattern.flags());
        assertEquals("[a]*", pattern.pattern());
    }

    @Test
    public void testObjectWithNestedArray() {
        final byte[] bytes = getBytes(new BasicBSONObject("l", Arrays.asList(1, 2, 3)));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(1, lazy.keySet().size());
        assertThat(lazy.get("l"), instanceOf(LazyBSONList.class));
        final LazyBSONList list = (LazyBSONList) lazy.get("l");
        assertEquals(3, list.size());
    }

    @Test
    public void testObjectWithNestedArrayFollowedByAField() {
        final byte[] bytes = getBytes(new BasicBSONObject("l", Arrays.asList(1, 2, 3)).append("a", true));
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(2, lazy.keySet().size());
        assertThat(lazy.get("l"), instanceOf(LazyBSONList.class));
        final LazyBSONList list = (LazyBSONList) lazy.get("l");
        assertEquals(3, list.size());
        assertEquals(true, lazy.get("a"));
    }

    @Test
    public void testObjectWithNestedArrayAndObject() {
        final byte[] bytes = getBytes(
                new BasicBSONObject("a", Arrays.asList(1, 2, 3))
                        .append("o", new BasicBSONObject("z", 0.1))
        );
        final LazyBSONObject lazy = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(2, lazy.keySet().size());
        assertThat(lazy.get("a"), instanceOf(LazyBSONList.class));
        assertThat(lazy.get("o"), instanceOf(LazyBSONObject.class));
        final BSONObject nested = (BSONObject) lazy.get("o");
        assertEquals(0.1, nested.get("z"));
    }

    @Test
    public void testGetBSONSize() {
        final byte[] bytes = new byte[]{12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0};
        final LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback());
        assertEquals(12, document.getBSONSize());
    }
}
