/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb;

import org.bson.LazyBSONCallback;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LazyWriteableDBObjectTest extends DatabaseTestCase {

    private DBObject document;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        byte[] bytes = new byte[]{12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0};
        document = new LazyWriteableDBObject(bytes, new LazyDBCallback(collection));
    }

    @Test
    public void testPut() {
        document.put("w", 3);
        assertTrue(document.containsField("w"));
        assertEquals(3, document.get("w"));
    }

    @Test
    public void testContainsField() {
        document.put("g", 2);
        assertTrue(document.containsField("a"));
        assertTrue(document.containsField("g"));
        assertFalse(document.containsField("z"));
    }

    @Test
    public void testPutAll() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("w", 3);
        map.put("q", 4);
        document.putAll(map);
        assertTrue(document.containsField("w"));
        assertTrue(document.containsField("q"));
    }

    @Test
    public void testRemoveField() {
        document.put("w", "foo");
        Object removed = document.removeField("w");
        assertEquals("foo", removed);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveFieldWithInexistantField() {
        document.put("w", "foo");
        document.removeField("z");
    }

    @Test
    public void testKeySet() {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("w", 3);
        map.put("q", 4);
        document.putAll(map);
        assertThat(document.keySet(), hasItems("w", "q", "a"));
    }

    @Test
    public void testCallbackCreateObject() {
        LazyBSONCallback callback = new LazyWriteableDBCallback(collection);
        Object document = callback.createObject(new byte[]{12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0}, 0);

        assertThat(document, instanceOf(LazyWriteableDBObject.class));
    }

    @Test
    public void testCallbackCreateDBRef() {
        LazyBSONCallback callback = new LazyWriteableDBCallback(collection);
        Object document = callback.createObject(new byte[]{28, 0, 0, 0, 16, 36, 105, 100, 0, 1, 0, 0, 0, 2,
                                                           36, 114, 101, 102, 0, 4, 0, 0, 0, 97, 46, 98, 0, 0},
                                                0);

        assertThat(document, instanceOf(DBRef.class));
    }
}
