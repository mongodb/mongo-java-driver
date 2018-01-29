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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.pojo.PropertyReflectionUtils.isGetter;


final class TypeData<T> implements TypeWithTypeParameters<T> {
    private final Class<T> type;
    private final List<TypeData<?>> typeParameters;

    /**
     * Creates a new builder for ClassTypeData
     *
     * @param type the class for the type
     * @param <T> the type
     * @return the builder
     */
    public static <T> Builder<T> builder(final Class<T> type) {
        return new Builder<T>(notNull("type", type));
    }

    public static TypeData<?> newInstance(final Method method) {
        if (isGetter(method)) {
            return newInstance(method.getGenericReturnType(), method.getReturnType());
        } else {
            return newInstance(method.getGenericParameterTypes()[0], method.getParameterTypes()[0]);
        }
    }

    public static TypeData<?> newInstance(final Field field) {
        return newInstance(field.getGenericType(), field.getType());
    }

    public static <T> TypeData<T> newInstance(final Type genericType, final Class<T> clazz) {
        TypeData.Builder<T> builder = TypeData.builder(clazz);
        if (genericType instanceof ParameterizedType) {
            ParameterizedType pType = (ParameterizedType) genericType;
            for (Type argType : pType.getActualTypeArguments()) {
                getNestedTypeData(builder, argType);
            }
        }
        return builder.build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> void getNestedTypeData(final TypeData.Builder<T> builder, final Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType pType = (ParameterizedType) type;
            TypeData.Builder paramBuilder = TypeData.builder((Class) pType.getRawType());
            for (Type argType : pType.getActualTypeArguments()) {
                getNestedTypeData(paramBuilder, argType);
            }
            builder.addTypeParameter(paramBuilder.build());
        } else if (type instanceof TypeVariable) {
            builder.addTypeParameter(TypeData.builder(Object.class).build());
        } else if (type instanceof Class) {
            builder.addTypeParameter(TypeData.builder((Class) type).build());
        }
    }

    /**
     * @return the class this {@code ClassTypeData} represents
     */
    @Override
    public Class<T> getType() {
        return type;
    }

    /**
     * @return the type parameters for the class
     */
    @Override
    public List<TypeData<?>> getTypeParameters() {
        return typeParameters;
    }

    /**
     * A builder for TypeData
     *
     * @param <T> the main type
     */
    public static final class Builder<T> {
        private final Class<T> type;
        private final List<TypeData<?>> typeParameters = new ArrayList<TypeData<?>>();

        private Builder(final Class<T> type) {
            this.type = type;
        }

        /**
         * Adds a type parameter
         *
         * @param typeParameter the type parameter
         * @param <S> the type of the type parameter
         * @return this
         */
        public <S> Builder<T> addTypeParameter(final TypeData<S> typeParameter) {
            typeParameters.add(notNull("typeParameter", typeParameter));
            return this;
        }

        /**
         * Adds multiple type parameters
         *
         * @param typeParameters the type parameters
         * @return this
         */
        public Builder<T> addTypeParameters(final List<TypeData<?>> typeParameters) {
            notNull("typeParameters", typeParameters);
            for (TypeData<?> typeParameter : typeParameters) {
                addTypeParameter(typeParameter);
            }
            return this;
        }

        /**
         * @return the class type data
         */
        public TypeData<T> build() {
            return new TypeData<T>(type, Collections.unmodifiableList(typeParameters));
        }
    }

    @Override
    public String toString() {
        String typeParams = typeParameters.isEmpty() ? ""
                : ", typeParameters=[" + nestedTypeParameters(typeParameters) + "]";
        return "TypeData{"
                + "type=" + type.getSimpleName()
                + typeParams
                + "}";
    }

    private static String nestedTypeParameters(final List<TypeData<?>> typeParameters) {
        StringBuilder builder = new StringBuilder();
        int count = 0;
        int last = typeParameters.size();
        for (TypeData<?> typeParameter : typeParameters) {
            count++;
            builder.append(typeParameter.getType().getSimpleName());
            if (!typeParameter.getTypeParameters().isEmpty()) {
                builder.append(format("<%s>", nestedTypeParameters(typeParameter.getTypeParameters())));
            }
            if (count < last) {
                builder.append(", ");
            }
        }
        return builder.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TypeData)) {
            return false;
        }

        TypeData<?> that = (TypeData<?>) o;

        if (!getType().equals(that.getType())) {
            return false;
        }
        if (!getTypeParameters().equals(that.getTypeParameters())) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = getType().hashCode();
        result = 31 * result + getTypeParameters().hashCode();
        return result;
    }

    private TypeData(final Class<T> type, final List<TypeData<?>> typeParameters) {
        this.type = boxType(type);
        this.typeParameters = typeParameters;
    }

    boolean isAssignableFrom(final Class<?> cls) {
        return type.isAssignableFrom(boxType(cls));
    }

    @SuppressWarnings("unchecked")
    private <S> Class<S> boxType(final Class<S> clazz) {
        if (clazz.isPrimitive()) {
            return (Class<S>) PRIMITIVE_CLASS_MAP.get(clazz);
        } else {
            return clazz;
        }
    }

    private static final Map<Class<?>, Class<?>> PRIMITIVE_CLASS_MAP;
    static {
        Map<Class<?>, Class<?>> map = new HashMap<Class<?>, Class<?>>();
        map.put(boolean.class, Boolean.class);
        map.put(byte.class, Byte.class);
        map.put(char.class, Character.class);
        map.put(double.class, Double.class);
        map.put(float.class, Float.class);
        map.put(int.class, Integer.class);
        map.put(long.class, Long.class);
        map.put(short.class, Short.class);
        PRIMITIVE_CLASS_MAP = map;
    }
}
