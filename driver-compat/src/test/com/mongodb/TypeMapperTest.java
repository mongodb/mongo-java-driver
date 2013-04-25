package com.mongodb;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
}
