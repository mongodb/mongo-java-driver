/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.MemberResolver;
import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.classmate.ResolvedTypeWithMembers;
import com.fasterxml.classmate.TypeResolver;
import com.fasterxml.classmate.members.ResolvedField;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class represents the various generics and field metadata of a class for use in mapping data to and from the database.
 */
public class ClassModel extends MappedType {
    private final Map<String, FieldModel> fields = new TreeMap<String, FieldModel>();
    private final CodecRegistry registry;
    private final TypeResolver resolver;

    /**
     * Construct a ClassModel for the given Classs.
     *
     * @param registry the registry to use for deferred lookups for codecs for the fields.
     * @param resolver the TypeResolver used in discovery of Class metatadata
     * @param aClass   the Class to model
     */
    public ClassModel(final CodecRegistry registry, final TypeResolver resolver, final Class<Object> aClass) {
        super(aClass);
        this.registry = registry;
        this.resolver = resolver;
        map();
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

    /**
     * Adds a field to the model
     *
     * @param field the field to add
     */
    public void addField(final FieldModel field) {
        fields.put(field.getName(), field);
    }

    /**
     * Retrieves a specific field from the model.
     *
     * @param name the field's name
     * @return the field
     */
    public FieldModel getField(final String name) {
        return fields.get(name);
    }

    /**
     * Returns all the fields on this model
     *
     * @return the list of fields
     */
    public List<FieldModel> getFields() {
        return new ArrayList<FieldModel>(fields.values());
    }
}
