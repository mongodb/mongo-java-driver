package org.bson.codecs.configuration.mapper;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PojoCodecTest {
    private static class Fooble<T> {
        private T value;
    }
    
    private static class Entity {
        private String name;
        private List<Integer> faves;
        private Map<String, Fooble<Double>> map;
    }

    @Test
    public void resolveEntityTypes() {
        final PojoCodec mapper = new PojoCodec();

        final MappedClass map = mapper.map(Entity.class);
        assertEquals("Should find 3 fields", 3, map.getFields().size());

        final MappedField field = map.getField("map");
        
//        assertEquals("Should have two parameter types", 2, field.getParameters().size());
    }
}