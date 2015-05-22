package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.TypeResolver;

import java.util.HashMap;
import java.util.Map;

public class PojoCodec {
    private final TypeResolver resolver = new TypeResolver();
    private final Map<Class, MappedClass> mapped = new HashMap<Class, MappedClass>();

    public MappedClass map(final Class<?> entityClass) {
        MappedClass mappedClass = mapped.get(entityClass);
        if(mappedClass == null) {
            mappedClass = new MappedClass(this, resolver, entityClass);
            mapped.put(entityClass, mappedClass);
        }


        return mappedClass;

    }
}
