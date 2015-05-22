package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.classmate.members.ResolvedField;

import java.lang.reflect.Field;
import java.util.List;

public class FieldModel extends MappedType {
    private final String name;
    private final ClassModel owner;
    private final Field field;

    public FieldModel(final PojoCodec pojoCodec, final ClassModel classModel, final ResolvedField field) {
        super(field.getType().getErasedType());
        owner = classModel;
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
