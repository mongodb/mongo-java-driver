package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.MemberResolver;
import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.classmate.ResolvedTypeWithMembers;
import com.fasterxml.classmate.TypeResolver;
import com.fasterxml.classmate.members.ResolvedField;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class MappedClass extends MappedType {
    private final Map<String, MappedField> fields = new TreeMap<String, MappedField>(); 

    public MappedClass(final PojoCodec pojoCodec, final TypeResolver resolver, final Class<?> aClass) {
        super(aClass);

        final ResolvedType type = resolver.resolve(aClass);
        final List<ResolvedType> resolvedTypes = type.getTypeParameters();
        for (final ResolvedType resolvedType : resolvedTypes) {
            addParameter(resolvedType.getErasedType());
        }
        
        final ResolvedTypeWithMembers bean = new MemberResolver(resolver)
                                                 .resolve(type, null, null);
        final ResolvedField[] fields = bean.getMemberFields();
        for (final ResolvedField field : fields) {
            addField(new MappedField(pojoCodec, this, field));
        }
        System.out.println("************ bean = " + bean);
        
    }

    public MappedField getField(final String name) {
        return fields.get(name);
    }

    public Collection<MappedField> getFields() {
        return fields.values();
    }

    public void addField(final MappedField field) {
        fields.put(field.getName(), field);
    }
}
