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

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;
import static java.util.Arrays.asList;
import static java.util.Collections.reverse;
import static org.bson.assertions.Assertions.notNull;

final class PojoBuilderHelper {

    @SuppressWarnings("unchecked")
    static <T> void configureClassModelBuilder(final ClassModelBuilder<T> classModelBuilder, final Class<T> clazz) {
        classModelBuilder.type(notNull("clazz", clazz));

        ArrayList<Annotation> annotations = new ArrayList<Annotation>();
        Set<String> fieldNames = new HashSet<String>();
        Map<String, TypeParameterMap> fieldTypeParameterMap = new HashMap<String, TypeParameterMap>();
        Class<? super T> currentClass = clazz;

        TypeData<?> parentClassTypeData = null;
        while (currentClass.getSuperclass() != null) {
            annotations.addAll(asList(currentClass.getDeclaredAnnotations()));

            List<String> genericTypeNames = new ArrayList<String>();
            for (TypeVariable<? extends Class<? super T>> classTypeVariable : currentClass.getTypeParameters()) {
                genericTypeNames.add(classTypeVariable.getName());
            }

            for (Field field : currentClass.getDeclaredFields()) {
                if (!fieldNames.add(field.getName()) || isTransient(field.getModifiers()) || isStatic(field.getModifiers())) {
                    continue;
                }
                TypeParameterMap typeParameterMap = getTypeParameterMap(genericTypeNames, field);
                fieldTypeParameterMap.put(field.getName(), typeParameterMap);

                FieldModelBuilder<?> fieldModelBuilder = getFieldBuilder(field, field.getType());
                if (parentClassTypeData != null) {
                    specializeFieldModelBuilder(fieldModelBuilder, typeParameterMap, parentClassTypeData.getTypeParameters());
                }

                classModelBuilder.addField(fieldModelBuilder);
                field.setAccessible(true);
            }

            parentClassTypeData = getTypeData(currentClass.getGenericSuperclass(), currentClass);
            currentClass = currentClass.getSuperclass();
        }

        reverse(annotations);
        classModelBuilder.annotations(annotations);
        classModelBuilder.fieldNameToTypeParameterMap(fieldTypeParameterMap);

        Constructor<T> noArgsConstructor = null;
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (constructor.getParameterTypes().length == 0) {
                noArgsConstructor = (Constructor<T>) constructor;
                noArgsConstructor.setAccessible(true);
            }
        }
        classModelBuilder.instanceCreatorFactory(new InstanceCreatorFactoryImpl<T>(clazz.getSimpleName(), noArgsConstructor));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T> TypeData<T> getTypeData(final Type genericType, final Class<T> clazz) {
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

    private static <T> FieldModelBuilder<T> getFieldBuilder(final Field field, final Class<T> clazz) {
        return FieldModel.<T>builder(field);
    }


    @SuppressWarnings("unchecked")
    static <T> FieldModelBuilder<T> configureFieldModelBuilder(final FieldModelBuilder<T> builder, final Field field) {
        return builder
                .fieldName(field.getName())
                .documentFieldName(field.getName())
                .typeData((TypeData<T>) getTypeData(field.getGenericType(), field.getType()))
                .annotations(asList(field.getDeclaredAnnotations()))
                .fieldSerialization(new FieldModelSerializationImpl<T>())
                .fieldAccessor(new FieldAccessorImpl<T>(field, field.getName()));
    }

    private static TypeParameterMap getTypeParameterMap(final List<String> genericTypeNames, final Field field) {
        int classParamIndex = genericTypeNames.indexOf(field.getGenericType().toString());
        TypeParameterMap.Builder builder = TypeParameterMap.builder();
        if (classParamIndex != -1) {
            builder.addIndex(classParamIndex);
        } else {
            Type type = field.getGenericType();
            if (type instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) type;
                for (int i = 0; i < pt.getActualTypeArguments().length; i++) {
                    classParamIndex = genericTypeNames.indexOf(pt.getActualTypeArguments()[i].toString());
                    if (classParamIndex != -1) {
                        builder.addIndex(i, classParamIndex);
                    }
                }
            }
        }
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private static <V> void specializeFieldModelBuilder(final FieldModelBuilder<V> fieldModelBuilder,
                                                        final TypeParameterMap typeParameterMap,
                                                        final List<TypeData<?>> fieldTypeParameters) {
        if (typeParameterMap.hasTypeParameters() && !fieldTypeParameters.isEmpty()) {
            TypeData<V> specializedFieldType = fieldModelBuilder.getTypeData();
            Map<Integer, Integer> fieldToClassParamIndexMap = typeParameterMap.getFieldToClassParamIndexMap();
            Integer classTypeParamRepresentsWholeField = fieldToClassParamIndexMap.get(-1);
            if (classTypeParamRepresentsWholeField != null) {
                specializedFieldType = (TypeData<V>) fieldTypeParameters.get(classTypeParamRepresentsWholeField);
            } else {
                TypeData.Builder<V> builder = TypeData.builder(fieldModelBuilder.getTypeData().getType());
                List<TypeData<?>> typeParameters = new ArrayList<TypeData<?>>(fieldModelBuilder.getTypeData().getTypeParameters());
                for (int i = 0; i < typeParameters.size(); i++) {
                    for (Map.Entry<Integer, Integer> mapping : fieldToClassParamIndexMap.entrySet()) {
                        if (mapping.getKey().equals(i)) {
                            typeParameters.set(i, fieldTypeParameters.get(mapping.getValue()));
                        }
                    }
                }
                builder.addTypeParameters(typeParameters);
                specializedFieldType = builder.build();
            }
            fieldModelBuilder.typeData(specializedFieldType);
        }
    }

    static <V> V stateNotNull(final String property, final V value) {
        if (value == null) {
            throw new IllegalStateException(format("%s cannot be null", property));
        }
        return value;
    }

    private PojoBuilderHelper() {
    }
}
