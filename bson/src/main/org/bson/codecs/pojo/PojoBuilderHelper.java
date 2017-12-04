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
import static org.bson.codecs.pojo.PropertyReflectionUtils.isGetter;
import static org.bson.codecs.pojo.PropertyReflectionUtils.getPropertyMethods;
import static org.bson.codecs.pojo.PropertyReflectionUtils.toPropertyName;

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

            PropertyReflectionUtils.PropertyMethods propertyMethods = getPropertyMethods(currentClass);

            // Note that we're processing setters before getters. It's typical for setters to have more general types
            // than getters (e.g.: getter returning ImmutableList, but setter accepting Collection), so by evaluating
            // setters first, we'll initialize the PropertyMetadata with the more general type
            for (Method method : propertyMethods.getSetterMethods()) {
                String propertyName = toPropertyName(method);
                propertyNames.add(propertyName);
                PropertyMetadata<?> propertyMetadata = getOrCreateProperty(propertyName, declaringClassName, propertyNameMap,
                        TypeData.newInstance(method), propertyTypeParameterMap, parentClassTypeData, genericTypeNames,
                        getGenericType(method), PropertyTypeCheck.THROW_ON_MISMATCH);
                if (propertyMetadata.getSetter() == null) {
                    propertyMetadata.setSetter(method);
                    for (Annotation annotation : method.getDeclaredAnnotations()) {
                        propertyMetadata.addWriteAnnotation(annotation);
                    }
                }
            }

            for (Method method : propertyMethods.getGetterMethods()) {
                String propertyName = toPropertyName(method);
                propertyNames.add(propertyName);
                // If the getter is overridden in a subclass, we only want to process that property, and ignore
                // potentially less specific methods from super classes
                PropertyMetadata<?> propertyMetadata = propertyNameMap.get(propertyName);
                if (propertyMetadata != null && propertyMetadata.getGetter() != null) {
                    continue;
                }
                propertyMetadata = getOrCreateProperty(propertyName, declaringClassName, propertyNameMap,
                        TypeData.newInstance(method), propertyTypeParameterMap, parentClassTypeData, genericTypeNames,
                        getGenericType(method), PropertyTypeCheck.THROW_ON_MISMATCH);

                if (propertyMetadata.getGetter() == null) {
                    propertyMetadata.setGetter(method);
                    for (Annotation annotation : method.getDeclaredAnnotations()) {
                        propertyMetadata.addReadAnnotation(annotation);
                    }
                }
            }

            for (Field field : currentClass.getDeclaredFields()) {
                propertyNames.add(field.getName());
                // Note we do a PropertyTypeCheck.NULL_ON_MISMATCH type check here. If properties are present, the
                // underlying field should be treated as an implementation detail, so we won't forcefully assert the
                // type matches, and similarly, won't use it for populating the model.
                PropertyMetadata<?> propertyMetadata = getOrCreateProperty(field.getName(), declaringClassName, propertyNameMap,
                        TypeData.newInstance(field), propertyTypeParameterMap, parentClassTypeData,
                        genericTypeNames, field.getGenericType(), PropertyTypeCheck.NULL_ON_MISMATCH);
                if (propertyMetadata != null && propertyMetadata.getField() == null) {
                    propertyMetadata.field(field);
                    for (Annotation annotation : field.getDeclaredAnnotations()) {
                        propertyMetadata.addReadAnnotation(annotation);
                        propertyMetadata.addWriteAnnotation(annotation);
                    }
                }
            }

            parentClassTypeData = TypeData.newInstance(currentClass.getGenericSuperclass(), currentClass);
            currentClass = currentClass.getSuperclass();
        }

        if (currentClass.isInterface()) {
            annotations.addAll(asList(currentClass.getDeclaredAnnotations()));
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
                                                                  final Type genericType,
                                                                  final PropertyTypeCheck propertyTypeCheck) {
        PropertyMetadata<T> propertyMetadata = (PropertyMetadata<T>) propertyNameMap.get(propertyName);
        if (propertyMetadata == null) {
            propertyMetadata = new PropertyMetadata<T>(propertyName, declaringClassName, typeData);
            propertyNameMap.put(propertyName, propertyMetadata);
        } else if (!propertyMetadata.getTypeData().getType().isAssignableFrom(typeData.getType())) {
            // This allows subsequent invocations for the same property to provide more specific types
            // (Collection -> ImmutableList). The patterns this method is called in ensures that ordering by evaluating
            // setters (often accept more general types) before getters (often returns more specific types)
            if (propertyTypeCheck == PropertyTypeCheck.THROW_ON_MISMATCH) {
                throw new CodecConfigurationException(format("Property '%s' in %s, has differing data types: %s and %s", propertyName,
                        declaringClassName, propertyMetadata.getTypeData(), typeData));
            } else {
                return null;
            }
        }
        TypeParameterMap typeParameterMap = getTypeParameterMap(genericTypeNames, genericType);
        propertyTypeParameterMap.put(propertyMetadata.getName(), typeParameterMap);
        propertyMetadata.typeParameterInfo(typeParameterMap, parentClassTypeData);
        return propertyMetadata;
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
            TypeData<V> specializedFieldType;
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

    private enum PropertyTypeCheck {
        /**
         * Throw an exception if the existing property type and incoming type don't match
         */
        THROW_ON_MISMATCH,
        /**
         * If the existing and incoming types don't match, don't throw and instead return null
         */
        NULL_ON_MISMATCH
    }

    private PojoBuilderHelper() {
    }
}
