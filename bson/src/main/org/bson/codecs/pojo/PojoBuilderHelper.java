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

import org.bson.codecs.configuration.CodecConfigurationException;

import java.beans.Introspector;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isProtected;
import static java.lang.reflect.Modifier.isPublic;
import static java.util.Arrays.asList;
import static java.util.Collections.reverse;
import static org.bson.assertions.Assertions.notNull;

final class PojoBuilderHelper {

    @SuppressWarnings("unchecked")
    static <T> void configureClassModelBuilder(final ClassModelBuilder<T> classModelBuilder, final Class<T> clazz) {
        classModelBuilder.type(notNull("clazz", clazz));

        ArrayList<Annotation> annotations = new ArrayList<Annotation>();
        Set<String> propertyNames = new TreeSet<String>();
        Map<String, TypeParameterMap> propertyTypeParameterMap = new HashMap<String, TypeParameterMap>();
        Class<? super T> currentClass = clazz;
        TypeData<?> parentClassTypeData = null;

        Map<String, PropertyMetadata<?>> propertyNameMap = new HashMap<String, PropertyMetadata<?>>();
        while (currentClass.getSuperclass() != null) {
            annotations.addAll(asList(currentClass.getDeclaredAnnotations()));
            List<String> genericTypeNames = new ArrayList<String>();
            for (TypeVariable<? extends Class<? super T>> classTypeVariable : currentClass.getTypeParameters()) {
                genericTypeNames.add(classTypeVariable.getName());
            }

            for (Method method : currentClass.getDeclaredMethods()) {
                String methodName = method.getName();
                if (isPropertyMethod(methodName) && isPublic(method.getModifiers())) {
                    String propertyName = toPropertyName(methodName);
                    propertyNames.add(propertyName);
                    PropertyMetadata<?> propertyMetadata = getOrCreateProperty(propertyName, propertyNameMap, getTypeData(method),
                            propertyTypeParameterMap, parentClassTypeData, genericTypeNames, getGenericType(method));
                    if (isGetter(methodName) && propertyMetadata.getGetter() == null) {
                        propertyMetadata.setGetter(method);
                        addAnnotations(propertyMetadata, method.getDeclaredAnnotations());
                    } else if (propertyMetadata.getSetter() == null) {
                        propertyMetadata.setSetter(method);
                        addAnnotations(propertyMetadata, method.getDeclaredAnnotations());
                    }
                }
            }

            for (Field field : currentClass.getDeclaredFields()) {
                propertyNames.add(field.getName());
                PropertyMetadata<?> propertyMetadata = getOrCreateProperty(field.getName(), propertyNameMap,
                        getTypeData(field.getGenericType(), field.getType()), propertyTypeParameterMap, parentClassTypeData,
                        genericTypeNames, field.getGenericType());
                if (propertyMetadata.getField() == null) {
                    propertyMetadata.field(field);
                    addAnnotations(propertyMetadata, field.getDeclaredAnnotations());
                }
            }

            parentClassTypeData = getTypeData(currentClass.getGenericSuperclass(), currentClass);
            currentClass = currentClass.getSuperclass();
        }

        for (String propertyName : propertyNames) {
            PropertyMetadata<?> propertyMetadata = propertyNameMap.get(propertyName);
            if (propertyMetadata.isSerializable()) {
                classModelBuilder.addProperty(createPropertyModelBuilder(propertyMetadata));
            }
        }

        reverse(annotations);
        classModelBuilder.annotations(annotations);
        classModelBuilder.propertyNameToTypeParameterMap(propertyTypeParameterMap);

        Constructor<T> noArgsConstructor = null;
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (constructor.getParameterTypes().length == 0 && isPublicOrProtected(constructor.getModifiers())) {
                noArgsConstructor = (Constructor<T>) constructor;
                noArgsConstructor.setAccessible(true);
            }
        }

        classModelBuilder.instanceCreatorFactory(new InstanceCreatorFactoryImpl<T>(new CreatorExecutable<T>(clazz, noArgsConstructor)));
    }

    @SuppressWarnings("unchecked")
    private static <T, S> PropertyMetadata<T> getOrCreateProperty(final String propertyName,
                                                                  final Map<String, PropertyMetadata<?>> propertyNameMap,
                                                                  final TypeData<T> typeData,
                                                                  final Map<String, TypeParameterMap> propertyTypeParameterMap,
                                                                  final TypeData<S> parentClassTypeData,
                                                                  final List<String> genericTypeNames,
                                                                  final Type genericType) {
        PropertyMetadata<T> propertyMetadata = (PropertyMetadata<T>) propertyNameMap.get(propertyName);
        if (propertyMetadata == null) {
            propertyMetadata = new PropertyMetadata<T>(propertyName, typeData);
            propertyNameMap.put(propertyName, propertyMetadata);
        }
        TypeParameterMap typeParameterMap = getTypeParameterMap(genericTypeNames, genericType);
        propertyTypeParameterMap.put(propertyMetadata.getName(), typeParameterMap);
        propertyMetadata.typeParameterInfo(typeParameterMap, parentClassTypeData);
        return propertyMetadata;
    }

    private static <T> void addAnnotations(final PropertyMetadata<T> propertyMetadata, final Annotation[] annotations) {
        for (Annotation annotation : annotations) {
            propertyMetadata.addAnnotation(annotation);
        }
    }

    private static boolean isPublicOrProtected(final int modifiers) {
        return isPublic(modifiers) || isProtected(modifiers);
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

    private static TypeData<?> getTypeData(final Method method) {
        if (isGetter(method.getName())) {
            return getTypeData(method.getGenericReturnType(), method.getReturnType());
        } else {
            if (method.getGenericParameterTypes().length != 1) {
                throw new CodecConfigurationException(format("Invalid count of arguments for setter method: %s",
                        method.getName()));
            }
            return getTypeData(method.getGenericParameterTypes()[0], method.getParameterTypes()[0]);
        }
    }

    private static Type getGenericType(final Method method) {
        return isGetter(method.getName()) ? method.getGenericReturnType() : method.getGenericParameterTypes()[0];
    }

    @SuppressWarnings("unchecked")
    static <T> PropertyModelBuilder<T> createPropertyModelBuilder(final PropertyMetadata<T> propertyMetadata) {
        PropertyModelBuilder<T> propertyModelBuilder = PropertyModel.<T>builder()
                .propertyName(propertyMetadata.getName())
                .documentPropertyName(propertyMetadata.getName())
                .typeData(propertyMetadata.getTypeData())
                .annotations(propertyMetadata.getAnnotations())
                .propertySerialization(new PropertyModelSerializationImpl<T>())
                .propertyAccessor(new PropertyAccessorImpl<T>(propertyMetadata));

        if (propertyMetadata.getTypeParameters() != null) {
            specializePropertyModelBuilder(propertyModelBuilder, propertyMetadata);
        }

        return propertyModelBuilder;
    }

    private static TypeParameterMap getTypeParameterMap(final List<String> genericTypeNames, final Type propertyType) {
        int classParamIndex = genericTypeNames.indexOf(propertyType.toString());
        TypeParameterMap.Builder builder = TypeParameterMap.builder();
        if (classParamIndex != -1) {
            builder.addIndex(classParamIndex);
        } else {
            if (propertyType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) propertyType;
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
    private static <V> void specializePropertyModelBuilder(final PropertyModelBuilder<V> propertyModelBuilder,
                                                           final PropertyMetadata<V> propertyMetadata) {
        if (propertyMetadata.getTypeParameterMap().hasTypeParameters() && !propertyMetadata.getTypeParameters().isEmpty()) {
            TypeData<V> specializedFieldType = propertyModelBuilder.getTypeData();
            Map<Integer, Integer> fieldToClassParamIndexMap = propertyMetadata.getTypeParameterMap().getPropertyToClassParamIndexMap();
            Integer classTypeParamRepresentsWholeField = fieldToClassParamIndexMap.get(-1);
            if (classTypeParamRepresentsWholeField != null) {
                specializedFieldType = (TypeData<V>) propertyMetadata.getTypeParameters().get(classTypeParamRepresentsWholeField);
            } else {
                TypeData.Builder<V> builder = TypeData.builder(propertyModelBuilder.getTypeData().getType());
                List<TypeData<?>> typeParameters = new ArrayList<TypeData<?>>(propertyModelBuilder.getTypeData().getTypeParameters());
                for (int i = 0; i < typeParameters.size(); i++) {
                    for (Map.Entry<Integer, Integer> mapping : fieldToClassParamIndexMap.entrySet()) {
                        if (mapping.getKey().equals(i)) {
                            typeParameters.set(i, propertyMetadata.getTypeParameters().get(mapping.getValue()));
                        }
                    }
                }
                builder.addTypeParameters(typeParameters);
                specializedFieldType = builder.build();
            }
            propertyModelBuilder.typeData(specializedFieldType);
        }
    }

    static <V> V stateNotNull(final String property, final V value) {
        if (value == null) {
            throw new IllegalStateException(format("%s cannot be null", property));
        }
        return value;
    }

    private static final String IS_PREFIX = "is";

    private static final String GET_PREFIX = "get";

    private static final String SET_PREFIX = "set";

    static boolean isSetter(final String methodName) {
        return methodName.startsWith(SET_PREFIX);
    }

    static boolean isGetter(final String methodName) {
        return methodName.startsWith(GET_PREFIX) || methodName.startsWith(IS_PREFIX);
    }

    static boolean isPropertyMethod(final String methodName) {
        return isGetter(methodName) || isSetter(methodName);
    }

    static String toPropertyName(final String name) {
        return Introspector.decapitalize(name.substring(name.startsWith(IS_PREFIX) ? 2 : 3, name.length()));
    }

    private PojoBuilderHelper() {
    }
}
