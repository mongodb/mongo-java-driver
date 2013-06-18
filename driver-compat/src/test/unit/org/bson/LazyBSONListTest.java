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

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("rawtypes")
public class LazyBSONListTest {
    private LazyBSONList encodeAndExtractList(List<?> list) {
        final BSONObject document = new BasicBSONObject("l", list);
        return (LazyBSONList) new LazyBSONObject(BSON.encode(document), new LazyBSONCallback()).get("l");
    }


    @Test
    public void testArray() {
        final LazyBSONList list = encodeAndExtractList(Arrays.asList(1, 2, 3));
        assertEquals(3, list.size());
        assertEquals(1, list.get(0));
        assertEquals(2, list.get(1));
        assertEquals(3, list.get(2));
    }

    @Test
    public void testEmptyArray() {
        final LazyBSONList list = encodeAndExtractList(Arrays.asList());
        assertEquals(0, list.size());
        assertFalse(list.iterator().hasNext());
    }

    @Test
    public void testIndexOf() {
        final LazyBSONList list = encodeAndExtractList(Arrays.asList("a", "b", "z"));
        assertEquals(0, list.indexOf("a"));
        assertEquals(2, list.indexOf("z"));
        assertEquals(-1, list.indexOf("y"));
    }

    @Test
    public void testLastIndexOf() {
        final LazyBSONList list = encodeAndExtractList(Arrays.asList("a", "b", "z", "b", "b", "z"));
        assertEquals(4, list.lastIndexOf("b"));
        assertEquals(5, list.lastIndexOf("z"));
        assertEquals(0, list.lastIndexOf("a"));
        assertEquals(-1, list.lastIndexOf("x"));
    }

    @Test
    public void testContainsAll() {
        final LazyBSONList list = encodeAndExtractList(Arrays.asList("a", "b", "z"));
        assertTrue(list.containsAll(Arrays.asList("a", "b")));
        assertFalse(list.containsAll(Arrays.asList("a", "b", "c", "z")));
    }

    @Test
    public void testIterator() {
        final LazyBSONList list = encodeAndExtractList(Arrays.asList("a", "b"));
        final Iterator it = list.iterator();
        assertTrue(it.hasNext());
        assertEquals("a", it.next());
        assertTrue(it.hasNext());
        assertEquals("b", it.next());
        assertFalse(it.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void testIteratorNextWhileNothingLeft() {
        final LazyBSONList list = encodeAndExtractList(Arrays.asList());
        final Iterator it = list.iterator();
        assertFalse(it.hasNext());
        it.next();
    }

}
