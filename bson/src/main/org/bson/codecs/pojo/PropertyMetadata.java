/*
 * Copyright 2008-present MongoDB, Inc.
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

package org.bson.codecs.pojo;

import org.bson.codecs.configuration.CodecConfigurationException;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;

final class PropertyMetadata<T> {
    private static final TypeData<Void> VOID_TYPE_DATA = TypeData.builder(Void.class).build();
    private final String name;
    private final String declaringClassName;
    private final TypeData<T> typeData;
    private final Map<Class<? extends Annotation>, Annotation> readAnnotations = new HashMap<>();
    private final Map<Class<? extends Annotation>, Annotation> writeAnnotations = new HashMap<>();
    private TypeParameterMap typeParameterMap;
    private List<TypeData<?>> typeParameters;

    private String error;
    private Field field;
    private Method getter;
    private Method setter;

    PropertyMetadata(final String name, final String declaringClassName, final TypeData<T> typeData) {
        this.name = name;
        this.declaringClassName = declaringClassName;
        this.typeData = typeData;
    }

    public String getName() {
        return name;
    }

    public List<Annotation> getReadAnnotations() {
        return new ArrayList<>(readAnnotations.values());
    }

    public PropertyMetadata<T> addReadAnnotation(final Annotation annotation) {
        if (readAnnotations.containsKey(annotation.annotationType())) {
            if (annotation.equals(readAnnotations.get(annotation.annotationType()))) {
                return this;
            }
            throw new CodecConfigurationException(format("Read annotation %s for '%s' already exists in %s", annotation.annotationType(),
                    name, declaringClassName));
        }
        readAnnotations.put(annotation.annotationType(), annotation);
        return this;
    }

    public List<Annotation> getWriteAnnotations() {
        return new ArrayList<>(writeAnnotations.values());
    }

    public PropertyMetadata<T> addWriteAnnotation(final Annotation annotation) {
        if (writeAnnotations.containsKey(annotation.annotationType())) {
            if (annotation.equals(writeAnnotations.get(annotation.annotationType()))) {
                return this;
            }
            throw new CodecConfigurationException(format("Write annotation %s for '%s' already exists in %s", annotation.annotationType(),
                    name, declaringClassName));
        }
        writeAnnotations.put(annotation.annotationType(), annotation);
        return this;
    }

    public Field getField() {
        return field;
    }

    public PropertyMetadata<T> field(final Field field) {
        this.field = field;
        return this;
    }

    public Method getGetter() {
        return getter;
    }

    public void setGetter(final Method getter) {
        this.getter = getter;
    }

    public Method getSetter() {
        return setter;
    }

    public void setSetter(final Method setter) {
        this.setter = setter;
    }

    public String getDeclaringClassName() {
        return declaringClassName;
    }

    public TypeData<T> getTypeData() {
        return typeData;
    }

    public TypeParameterMap getTypeParameterMap() {
        return typeParameterMap;
    }

    public List<TypeData<?>> getTypeParameters() {
        return typeParameters;
    }

    public <S> PropertyMetadata<T> typeParameterInfo(final TypeParameterMap typeParameterMap, final TypeData<S> parentTypeData) {
        if (typeParameterMap != null && parentTypeData != null) {
            this.typeParameterMap = typeParameterMap;
            this.typeParameters = parentTypeData.getTypeParameters();
        }
        return this;
    }

    String getError() {
        return error;
    }

    void setError(final String error) {
        this.error = error;
    }

    public boolean isSerializable() {
        if (isVoidType()) {
            return false;
        }
        if (getter != null) {
            return field == null || notStaticOrTransient(field.getModifiers());
        } else {
            return field != null && isPublicAndNotStaticOrTransient(field.getModifiers());
        }
    }

    public boolean isDeserializable() {
        if (isVoidType()) {
            return false;
        }
        if (setter != null) {
            return field == null || !isFinal(field.getModifiers()) && notStaticOrTransient(field.getModifiers());
        } else {
            return field != null && !isFinal(field.getModifiers()) && isPublicAndNotStaticOrTransient(field.getModifiers());
        }
    }

    private boolean isVoidType() {
        return VOID_TYPE_DATA.equals(typeData);
    }

    private boolean notStaticOrTransient(final int modifiers) {
        return !(isTransient(modifiers) || isStatic(modifiers));
    }

    private boolean isPublicAndNotStaticOrTransient(final int modifiers) {
        return isPublic(modifiers) && notStaticOrTransient(modifiers);
    }

    @Override
    public String toString() {
        return "PropertyMetadata{"
                + "name='" + name + '\''
                + ", declaringClassName='" + declaringClassName + '\''
                + ", typeData=" + typeData
                + ", readAnnotations=" + readAnnotations
                + ", writeAnnotations=" + writeAnnotations
                + ", typeParameterMap=" + typeParameterMap
                + ", typeParameters=" + typeParameters
                + ", error='" + error + '\''
                + ", field=" + field
                + ", getter=" + getter
                + ", setter=" + setter
                + '}';
    }
}
