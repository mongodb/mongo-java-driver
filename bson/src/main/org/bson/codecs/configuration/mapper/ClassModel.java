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

public class ClassModel extends MappedType {
    private final Map<String, FieldModel> fields = new TreeMap<String, FieldModel>(); 

    public ClassModel(final PojoCodec pojoCodec, final TypeResolver resolver, final Class<?> aClass) {
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
            addField(new FieldModel(pojoCodec, this, field));
        }
        System.out.println("************ bean = " + bean);
        
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
