package com.mongodb;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TypeMapperTest {

    @Test
    public void testGetters() {
        final TypeMapper typeMapper = new TypeMapper(BasicDBObject.class);
        assertEquals(BasicDBObject.class, typeMapper.getTopLevelClass());
        final HashMap<List<String>, Class<? extends DBObject>> expected
                = new HashMap<List<String>, Class<? extends DBObject>>();
        expected.put(new ArrayList<String>(), BasicDBObject.class);
        assertEquals(expected, typeMapper.getPathToClassMap());
    }

    @Test
    public void testPathToClassMap() {
        final Map<String, Class<? extends DBObject>> stringPathToClassMap
                = new HashMap<String, Class<? extends DBObject>>();
        stringPathToClassMap.put("a", NestedOneDBObject.class);
        stringPathToClassMap.put("a.b", NestedTwoDBObject.class);

        final TypeMapper typeMapper = new TypeMapper(TopLevelDBObject.class, stringPathToClassMap);

        final Map<List<String>, Class<? extends DBObject>> pathToClassMap
                = new HashMap<List<String>, Class<? extends DBObject>>();
        pathToClassMap.put(new ArrayList<String>(), TopLevelDBObject.class);
        pathToClassMap.put(Arrays.asList("a"), NestedOneDBObject.class);
        pathToClassMap.put(Arrays.asList("a", "b"), NestedTwoDBObject.class);
        assertEquals(pathToClassMap, typeMapper.getPathToClassMap());
    }

    public static class TopLevelDBObject extends BasicDBObject {
        private static final long serialVersionUID = 7029929727222305692L;
    }

    public static class NestedOneDBObject extends BasicDBObject {
        private static final long serialVersionUID = -5821458746671670383L;
    }

    public static class NestedTwoDBObject extends BasicDBObject {
        private static final long serialVersionUID = 5243874721805359328L;
    }

}
