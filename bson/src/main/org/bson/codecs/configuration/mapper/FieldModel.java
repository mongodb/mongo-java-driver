package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.classmate.members.ResolvedField;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.util.List;

import static org.slf4j.LoggerFactory.getLogger;

public class FieldModel extends MappedType {
    private final String name;
    private final ClassModel owner;
    private final CodecRegistry registry;
    private final Field field;
    private Codec codec;

    public FieldModel(final ClassModel classModel, final CodecRegistry registry, final ResolvedField field) {
        super(field.getType().getErasedType());
        owner = classModel;
        this.registry = registry;
        this.field = field.getRawMember();
        this.field.setAccessible(true);
        this.name = field.getName();
        final List<ResolvedType> typeParameters = field.getType().getTypeParameters();
        for (final ResolvedType parameter : typeParameters) {
            addParameter(parameter.getErasedType());
        }
    }
    
    public Object get(final Object value) {
        try {
            return field.get(value);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void set(final Object entity, final Object value) {
        try {
            field.set(entity, value);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    Codec getCodec() {
        if ( codec == null ) {
            codec = registry.get(field.getType());
        }
        return codec;
    }

    public String getName() {
        return name;
    }

}
