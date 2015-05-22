package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.TypeResolver;

import java.util.HashMap;
import java.util.Map;

public class PojoCodec {
    private final TypeResolver resolver = new TypeResolver();
    private final Map<Class, ClassModel> mapped = new HashMap<Class, ClassModel>();

    public ClassModel map(final Class<?> entityClass) {
        ClassModel classModel = mapped.get(entityClass);
        if(classModel == null) {
            classModel = new ClassModel(this, resolver, entityClass);
            mapped.put(entityClass, classModel);
        }


        return classModel;

    }
}
