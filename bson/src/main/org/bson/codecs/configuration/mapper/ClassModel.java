package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.MemberResolver;
import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.classmate.ResolvedTypeWithMembers;
import com.fasterxml.classmate.TypeResolver;
import com.fasterxml.classmate.members.ResolvedField;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ClassModel extends MappedType {
    private final Map<String, FieldModel> fields = new TreeMap<String, FieldModel>();
    private final CodecRegistry registry;
    private final TypeResolver resolver;
    
    public ClassModel(final CodecRegistry registry, final TypeResolver resolver, final Class<?> aClass) {
        super(aClass);
        this.registry = registry;
        this.resolver = resolver;
        map();
    }

    public <T> T decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartDocument();
        try {
            final Object entity = getType().newInstance();
            for (final FieldModel fieldModel : fields.values()) {
                fieldModel.decode(reader, entity, decoderContext);
            }
            return (T) entity;
        } catch (final Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public void encode(final BsonWriter writer, final Object value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        for (final FieldModel fieldModel : fields.values()) {
            fieldModel.encode(writer, value, encoderContext);
        }
        writer.writeEndDocument();
    }

    protected void map() {
        final ResolvedType type = resolver.resolve(getType());
        final List<ResolvedType> resolvedTypes = type.getTypeParameters();
        for (final ResolvedType resolvedType : resolvedTypes) {
            addParameter(resolvedType.getErasedType());
        }
        
        final ResolvedTypeWithMembers bean = new MemberResolver(resolver)
                                                 .resolve(type, null, null);
        final ResolvedField[] fields = bean.getMemberFields();
        for (final ResolvedField field : fields) {
            addField(new FieldModel(this, registry, field));
        }
    }
    
    public FieldModel getField(final String name) {
        return fields.get(name);
    }

    public Collection<FieldModel> getFields() {
        return fields.values();
    }

    public void addField(final FieldModel field) {
        fields.put(field.getName(), field);
    }
}
