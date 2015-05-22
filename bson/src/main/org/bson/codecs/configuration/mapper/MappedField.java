package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.classmate.members.ResolvedField;

import java.lang.reflect.Field;
import java.util.List;

public class MappedField extends MappedType {
    private final String name;
    private final MappedClass owner;
    private final Field field;

    public MappedField(final PojoCodec pojoCodec, final MappedClass mappedClass, final ResolvedField field) {
        super(field.getType().getErasedType());
        owner = mappedClass;
        this.field = field.getRawMember();
        this.name = field.getName();
        final List<ResolvedType> typeParameters = field.getType().getTypeParameters();
        for (final ResolvedType parameter : typeParameters) {
            addParameter(parameter.getErasedType());
        }
    }

    public String getName() {
        return name;
    }

}
