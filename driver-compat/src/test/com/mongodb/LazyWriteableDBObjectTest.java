package com.mongodb;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LazyWriteableDBObjectTest extends DatabaseTestCase {

    private DBObject document;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        final byte[] bytes = new byte[]{12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0};
        document = new LazyWriteableDBObject(bytes, new LazyDBCallback(collection));
    }

    @Test
    public void testPut() {
        document.put("w", 3);
        assertTrue(document.containsField("w"));
        assertEquals(3, document.get("w"));
    }

    @Test
    public void testPutAll() {
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("w", 3);
        map.put("q", 4);
        document.putAll(map);
        assertTrue(document.containsField("w"));
        assertTrue(document.containsField("q"));
    }

    @Test
    public void testRemoveField() {
        document.put("w", "foo");
        final Object removed = document.removeField("w");
        assertEquals("foo", removed);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveFieldWithInexistantField() {
        document.put("w", "foo");
        document.removeField("z");
    }

    @Test
    public void testKeySet() {
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("w", 3);
        map.put("q", 4);
        document.putAll(map);
        assertThat(document.keySet(), hasItems("w", "q", "a"));
    }
}
