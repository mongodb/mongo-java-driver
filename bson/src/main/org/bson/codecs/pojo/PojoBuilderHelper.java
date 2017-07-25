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
        String declaringClassName =  clazz.getSimpleName();
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
                if (isPropertyMethod(method) && isPublic(method.getModifiers())) {
                    String propertyName = toPropertyName(methodName);
                    propertyNames.add(propertyName);
                    PropertyMetadata<?> propertyMetadata = getOrCreateProperty(propertyName, declaringClassName, propertyNameMap,
                            getTypeData(method), propertyTypeParameterMap, parentClassTypeData, genericTypeNames, getGenericType(method));
                    if (isGetter(method) && propertyMetadata.getGetter() == null) {
                        propertyMetadata.setGetter(method);
                        for (Annotation annotation : method.getDeclaredAnnotations()) {
                            propertyMetadata.addReadAnnotation(annotation);
                        }
                    } else if (propertyMetadata.getSetter() == null) {
                        propertyMetadata.setSetter(method);
                        for (Annotation annotation : method.getDeclaredAnnotations()) {
                            propertyMetadata.addWriteAnnotation(annotation);
                        }
                    }
                }
            }

            for (Field field : currentClass.getDeclaredFields()) {
                propertyNames.add(field.getName());
                PropertyMetadata<?> propertyMetadata = getOrCreateProperty(field.getName(), declaringClassName, propertyNameMap,
                        getTypeData(field.getGenericType(), field.getType()), propertyTypeParameterMap, parentClassTypeData,
                        genericTypeNames, field.getGenericType());
                if (propertyMetadata.getField() == null) {
                    propertyMetadata.field(field);
                    for (Annotation annotation : field.getDeclaredAnnotations()) {
                        propertyMetadata.addReadAnnotation(annotation);
                        propertyMetadata.addWriteAnnotation(annotation);
                    }
                }
            }

            parentClassTypeData = getTypeData(currentClass.getGenericSuperclass(), currentClass);
            currentClass = currentClass.getSuperclass();
        }

        for (String propertyName : propertyNames) {
            PropertyMetadata<?> propertyMetadata = propertyNameMap.get(propertyName);
            if (propertyMetadata.isSerializable() || propertyMetadata.isDeserializable()) {
                classModelBuilder.addProperty(createPropertyModelBuilder(propertyMetadata));
            }
        }

        reverse(annotations);
        classModelBuilder.annotations(annotations);
        classModelBuilder.propertyNameToTypeParameterMap(propertyTypeParameterMap);

        Constructor<T> noArgsConstructor = null;
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (constructor.getParameterTypes().length == 0
                    && (isPublic(constructor.getModifiers()) || isProtected(constructor.getModifiers()))) {
                noArgsConstructor = (Constructor<T>) constructor;
                noArgsConstructor.setAccessible(true);
            }
        }

        classModelBuilder.instanceCreatorFactory(new InstanceCreatorFactoryImpl<T>(new CreatorExecutable<T>(clazz, noArgsConstructor)));
    }

    @SuppressWarnings("unchecked")
    private static <T, S> PropertyMetadata<T> getOrCreateProperty(final String propertyName,
                                                                  final String declaringClassName,
                                                                  final Map<String, PropertyMetadata<?>> propertyNameMap,
                                                                  final TypeData<T> typeData,
                                                                  final Map<String, TypeParameterMap> propertyTypeParameterMap,
                                                                  final TypeData<S> parentClassTypeData,
                                                                  final List<String> genericTypeNames,
                                                                  final Type genericType) {
        PropertyMetadata<T> propertyMetadata = (PropertyMetadata<T>) propertyNameMap.get(propertyName);
        if (propertyMetadata == null) {
            propertyMetadata = new PropertyMetadata<T>(propertyName, declaringClassName, typeData);
            propertyNameMap.put(propertyName, propertyMetadata);
        }
        if (!propertyMetadata.getTypeData().equals(typeData)) {
            throw new CodecConfigurationException(format("Property '%s' in %s, has differing data types: %s and %s", propertyName,
                    declaringClassName, propertyMetadata.getTypeData(), typeData));
        }
        TypeParameterMap typeParameterMap = getTypeParameterMap(genericTypeNames, genericType);
        propertyTypeParameterMap.put(propertyMetadata.getName(), typeParameterMap);
        propertyMetadata.typeParameterInfo(typeParameterMap, parentClassTypeData);
        return propertyMetadata;
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
        if (isGetter(method)) {
            return getTypeData(method.getGenericReturnType(), method.getReturnType());
        } else {
            return getTypeData(method.getGenericParameterTypes()[0], method.getParameterTypes()[0]);
        }
    }

    private static Type getGenericType(final Method method) {
        return isGetter(method) ? method.getGenericReturnType() : method.getGenericParameterTypes()[0];
    }

    @SuppressWarnings("unchecked")
    static <T> PropertyModelBuilder<T> createPropertyModelBuilder(final PropertyMetadata<T> propertyMetadata) {
        PropertyModelBuilder<T> propertyModelBuilder = PropertyModel.<T>builder()
                .propertyName(propertyMetadata.getName())
                .readName(propertyMetadata.getName())
                .writeName(propertyMetadata.getName())
                .typeData(propertyMetadata.getTypeData())
                .readAnnotations(propertyMetadata.getReadAnnotations())
                .writeAnnotations(propertyMetadata.getWriteAnnotations())
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

    static boolean isSetter(final Method method) {
        if (method.getName().startsWith(SET_PREFIX) && method.getName().length() > SET_PREFIX.length()
            && method.getParameterTypes().length == 1) {
            return Character.isUpperCase(method.getName().charAt(SET_PREFIX.length()));
        }
        return false;
    }

    static boolean isGetter(final Method method) {
        if (method.getParameterTypes().length > 0) {
            return false;
        } else if (method.getName().startsWith(GET_PREFIX) && method.getName().length() > GET_PREFIX.length()) {
            return Character.isUpperCase(method.getName().charAt(GET_PREFIX.length()));
        } else if (method.getName().startsWith(IS_PREFIX) && method.getName().length() > IS_PREFIX.length()) {
            return Character.isUpperCase(method.getName().charAt(IS_PREFIX.length()));
        }
        return false;
    }

    static boolean isPropertyMethod(final Method method) {
        return isGetter(method) || isSetter(method);
    }

    static String toPropertyName(final String name) {
        String propertyName = name.substring(name.startsWith(IS_PREFIX) ? 2 : 3, name.length());
        char[] chars = propertyName.toCharArray();
        chars[0] = Character.toLowerCase(chars[0]);
        return new String(chars);
    }

    private PojoBuilderHelper() {
    }
}
