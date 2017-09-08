/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;


final class TypeData<T> {
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

    /**
     * @return the class this {@code ClassTypeData} represents
     */
    public Class<T> getType() {
        return type;
    }

    /**
     * @return the type parameters for the class
     */
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
            validate();
            return new TypeData<T>(type, Collections.unmodifiableList(typeParameters));
        }

        private void validate() {
            if (Collection.class.isAssignableFrom(type)) {
                if (typeParameters.size() == 0) {
                    for (Type interfaceType : type.getGenericInterfaces()) {
                        if (interfaceType instanceof ParameterizedType) {
                            ParameterizedType pType = (ParameterizedType) interfaceType;
                            if (Collection.class.equals((Class<?>) pType.getRawType())) {
                                Type rawListType = pType.getActualTypeArguments()[0];
                                if (!(rawListType instanceof Class<?>)) {
                                    throw new IllegalStateException("Invalid Collection type. Collections must have a defined type.");
                                }
                            }
                        }
                    }
                } else if (typeParameters.size() > 1) {
                    throw new IllegalStateException("Invalid Collection type. Collections must have a single type parameter defined.");
                }
            } else if (Map.class.isAssignableFrom(type)) {
                Class<?> keyType = Object.class;
                if (typeParameters.size() != 2) {
                    for (Type interfaceType : type.getGenericInterfaces()) {
                        if (interfaceType instanceof ParameterizedType) {
                            ParameterizedType pType = (ParameterizedType) interfaceType;
                            if (Map.class.equals((Class<?>) pType.getRawType())) {
                                Type rawKeyType = pType.getActualTypeArguments()[0];
                                if (!(rawKeyType instanceof Class<?>)) {
                                    throw new IllegalStateException("Invalid Map type. Maps MUST have string keys");
                                } else {
                                    keyType = (Class<?>) rawKeyType;
                                }
                            }
                        }
                    }
                } else {
                    keyType = typeParameters.get(0).getType();
                }
                if (!keyType.equals(String.class)) {
                    throw new IllegalStateException(format("Invalid Map type. Maps MUST have string keys, found %s instead.", keyType));
                }
            }
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
        this.type = getClass(type);
        this.typeParameters = typeParameters;
    }

    @SuppressWarnings("unchecked")
    private Class<T> getClass(final Class<T> type) {
        Class<T> instanceType = boxType(type);
        if (type.equals(Map.class)) {
            instanceType = (Class<T>) HashMap.class;
        } else if (type.equals(List.class) || type.equals(Collection.class)) {
            instanceType = (Class<T>) ArrayList.class;
        } else if (type.equals(Set.class)) {
            instanceType = (Class<T>) HashSet.class;
        }
        return instanceType;
    }

    @SuppressWarnings("unchecked")
    private Class<T> boxType(final Class<T> clazz) {
        if (clazz.isPrimitive()) {
            return (Class<T>) PRIMITIVE_CLASS_MAP.get(clazz);
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
