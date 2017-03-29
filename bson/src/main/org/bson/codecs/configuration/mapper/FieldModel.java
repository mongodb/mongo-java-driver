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

import com.fasterxml.classmate.ResolvedType;
import com.fasterxml.classmate.members.ResolvedField;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;

import java.lang.reflect.Field;
import java.util.List;

/**
 * Represents a field on a class and stores various metadata such as generic parameters for use by the {@link ClassModelCodec}
 */
@SuppressWarnings("unchecked")
public class FieldModel extends MappedType {
    private final String name;
    private final ClassModel owner;
    private final CodecRegistry registry;
    private final Field field;
    private Codec<Object> codec;

    /**
     * Create the FieldModel
     *
     * @param classModel the owning ClassModel
     * @param registry   the TypeRegistry used to cache type information
     * @param field      the field to model
     */
    public FieldModel(final ClassModel classModel, final CodecRegistry registry, final ResolvedField field) {
        super((Class<Object>) field.getType().getErasedType());
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

    /**
     * Gets the value of the field from the given reference.
     *
     * @param entity the entity from which to pull the value
     * @return the value of the field
     */
    public Object get(final Object entity) {
        try {
            return field.get(entity);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    /**
     * @return the name of the mapped field
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the field on entity with the given value
     *
     * @param entity the entity to update
     * @param value  the value to set
     */
    public void set(final Object entity, final Object value) {
        try {
            field.set(entity, value);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    Codec<Object> getCodec() {
        if (codec == null) {
            codec = (Codec<Object>) registry.get(field.getType());
        }
        return codec;
    }

}
