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

import org.bson.BsonType;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.pojo.annotations.BsonCreator;
import org.bson.codecs.pojo.annotations.BsonDiscriminator;
import org.bson.codecs.pojo.annotations.BsonExtraElements;
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonIgnore;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.codecs.pojo.annotations.BsonRepresentation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;
import static org.bson.codecs.pojo.PojoBuilderHelper.createPropertyModelBuilder;

final class ConventionAnnotationImpl implements Convention {

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        for (final Annotation annotation : classModelBuilder.getAnnotations()) {
            processClassAnnotation(classModelBuilder, annotation);
        }

        for (PropertyModelBuilder<?> propertyModelBuilder : classModelBuilder.getPropertyModelBuilders()) {
            processPropertyAnnotations(classModelBuilder, propertyModelBuilder);
        }

        processCreatorAnnotation(classModelBuilder);

        cleanPropertyBuilders(classModelBuilder);
    }

    private void processClassAnnotation(final ClassModelBuilder<?> classModelBuilder, final Annotation annotation) {
        if (annotation instanceof BsonDiscriminator) {
            BsonDiscriminator discriminator = (BsonDiscriminator) annotation;
            String key = discriminator.key();
            if (!key.equals("")) {
                classModelBuilder.discriminatorKey(key);
            }

            String name = discriminator.value();
            if (!name.equals("")) {
                classModelBuilder.discriminator(name);
            }
            classModelBuilder.enableDiscriminator(true);
        }
    }

    private void processPropertyAnnotations(final ClassModelBuilder<?> classModelBuilder,
                                            final PropertyModelBuilder<?> propertyModelBuilder) {
        for (Annotation annotation : propertyModelBuilder.getReadAnnotations()) {
            if (annotation instanceof BsonProperty) {
                BsonProperty bsonProperty = (BsonProperty) annotation;
                if (!"".equals(bsonProperty.value())) {
                    propertyModelBuilder.readName(bsonProperty.value());
                }
                propertyModelBuilder.discriminatorEnabled(bsonProperty.useDiscriminator());
                if (propertyModelBuilder.getName().equals(classModelBuilder.getIdPropertyName())) {
                    classModelBuilder.idPropertyName(null);
                }
            } else if (annotation instanceof BsonId) {
                classModelBuilder.idPropertyName(propertyModelBuilder.getName());
            } else if (annotation instanceof BsonIgnore) {
                propertyModelBuilder.readName(null);
            } else if (annotation instanceof BsonRepresentation) {
                BsonRepresentation bsonRepresentation = (BsonRepresentation) annotation;
                BsonType bsonRep =  bsonRepresentation.value();
                propertyModelBuilder.bsonRepresentation(bsonRep);
            } else if (annotation instanceof BsonExtraElements) {
                processBsonExtraElementsAnnotation(propertyModelBuilder);
            }
        }

        for (Annotation annotation : propertyModelBuilder.getWriteAnnotations()) {
            if (annotation instanceof BsonProperty) {
                BsonProperty bsonProperty = (BsonProperty) annotation;
                if (!"".equals(bsonProperty.value())) {
                    propertyModelBuilder.writeName(bsonProperty.value());
                }
            } else if (annotation instanceof BsonIgnore) {
                propertyModelBuilder.writeName(null);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void processCreatorAnnotation(final ClassModelBuilder<T> classModelBuilder) {
        Class<T> clazz = classModelBuilder.getType();
        CreatorExecutable<T> creatorExecutable = null;
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (isPublic(constructor.getModifiers()) && !constructor.isSynthetic()) {
                for (Annotation annotation : constructor.getDeclaredAnnotations()) {
                    if (annotation.annotationType().equals(BsonCreator.class)) {
                        if (creatorExecutable != null) {
                            throw new CodecConfigurationException("Found multiple constructors annotated with @BsonCreator");
                        }
                        creatorExecutable = new CreatorExecutable<>(clazz, (Constructor<T>) constructor);
                    }
                }
            }
        }

        Class<?> bsonCreatorClass = clazz;
        boolean foundStaticBsonCreatorMethod = false;
        while (bsonCreatorClass != null && !foundStaticBsonCreatorMethod) {
            for (Method method : bsonCreatorClass.getDeclaredMethods()) {
                if (isStatic(method.getModifiers()) && !method.isSynthetic() && !method.isBridge()) {
                    for (Annotation annotation : method.getDeclaredAnnotations()) {
                        if (annotation.annotationType().equals(BsonCreator.class)) {
                            if (creatorExecutable != null) {
                                throw new CodecConfigurationException("Found multiple constructors / methods annotated with @BsonCreator");
                            } else if (!bsonCreatorClass.isAssignableFrom(method.getReturnType())) {
                                throw new CodecConfigurationException(
                                        format("Invalid method annotated with @BsonCreator. Returns '%s', expected %s",
                                                method.getReturnType(), bsonCreatorClass));
                            }
                            creatorExecutable = new CreatorExecutable<>(clazz, method);
                            foundStaticBsonCreatorMethod = true;
                        }
                    }
                }
            }

            bsonCreatorClass = bsonCreatorClass.getSuperclass();
        }

        if (creatorExecutable != null) {
            List<BsonProperty> properties = creatorExecutable.getProperties();
            List<Class<?>> parameterTypes = creatorExecutable.getParameterTypes();
            List<Type> parameterGenericTypes = creatorExecutable.getParameterGenericTypes();

            if (properties.size() != parameterTypes.size()) {
                throw creatorExecutable.getError(clazz, "All parameters in the @BsonCreator method / constructor must be annotated "
                                + "with a @BsonProperty.");
            }
            for (int i = 0; i < properties.size(); i++) {
                boolean isIdProperty = creatorExecutable.getIdPropertyIndex() != null && creatorExecutable.getIdPropertyIndex().equals(i);
                Class<?> parameterType = parameterTypes.get(i);
                Type genericType = parameterGenericTypes.get(i);
                PropertyModelBuilder<?> propertyModelBuilder = null;

                if (isIdProperty) {
                    propertyModelBuilder = classModelBuilder.getProperty(classModelBuilder.getIdPropertyName());
                } else {
                    BsonProperty bsonProperty = properties.get(i);

                    // Find the property using write name and falls back to read name
                    for (PropertyModelBuilder<?> builder : classModelBuilder.getPropertyModelBuilders()) {
                        if (bsonProperty.value().equals(builder.getWriteName())) {
                            propertyModelBuilder = builder;
                            break;
                        } else if (bsonProperty.value().equals(builder.getReadName())) {
                            // When there is a property that matches the read name of the parameter, save it but continue to look
                            // This is just in case there is another property that matches the write name.
                            propertyModelBuilder = builder;
                        }
                    }

                    // Support legacy options, when BsonProperty matches the actual POJO property name (e.g. method name or field name).
                    if (propertyModelBuilder == null) {
                        propertyModelBuilder = classModelBuilder.getProperty(bsonProperty.value());
                    }

                    if (propertyModelBuilder == null) {
                        propertyModelBuilder = addCreatorPropertyToClassModelBuilder(classModelBuilder, bsonProperty.value(),
                            parameterType);
                    } else {
                        // If not using a legacy BsonProperty reference to the property set the write name to be the annotated name.
                        if (!bsonProperty.value().equals(propertyModelBuilder.getName())) {
                            propertyModelBuilder.writeName(bsonProperty.value());
                        }
                        tryToExpandToGenericType(parameterType, propertyModelBuilder, genericType);
                    }
                }

                if (!propertyModelBuilder.getTypeData().isAssignableFrom(parameterType)) {
                    throw creatorExecutable.getError(clazz, format("Invalid Property type for '%s'. Expected %s, found %s.",
                        propertyModelBuilder.getWriteName(), propertyModelBuilder.getTypeData().getType(), parameterType));
                }
            }
            classModelBuilder.instanceCreatorFactory(new InstanceCreatorFactoryImpl<>(creatorExecutable));
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> void tryToExpandToGenericType(final Class<?> parameterType, final PropertyModelBuilder<T> propertyModelBuilder,
                                                     final Type genericType) {
        if (parameterType.isAssignableFrom(propertyModelBuilder.getTypeData().getType())) {
            // The existing getter for this field returns a more specific type than what the constructor accepts
            // This is typical when the getter returns a specific subtype, but the constructor accepts a more
            // general one (e.g.: getter returns ImmutableList<T>, while constructor just accepts List<T>)
            propertyModelBuilder.typeData(TypeData.newInstance(genericType, (Class<T>) parameterType));
        }
    }

    private <T, S> PropertyModelBuilder<S> addCreatorPropertyToClassModelBuilder(final ClassModelBuilder<T> classModelBuilder,
                                                                                 final String name,
                                                                                 final Class<S> clazz) {
        PropertyModelBuilder<S> propertyModelBuilder = createPropertyModelBuilder(new PropertyMetadata<>(name,
                classModelBuilder.getType().getSimpleName(), TypeData.builder(clazz).build())).readName(null).writeName(name);
        classModelBuilder.addProperty(propertyModelBuilder);
        return propertyModelBuilder;
    }

    private void cleanPropertyBuilders(final ClassModelBuilder<?> classModelBuilder) {
        List<String> propertiesToRemove = new ArrayList<>();
        for (PropertyModelBuilder<?> propertyModelBuilder : classModelBuilder.getPropertyModelBuilders()) {
            if (!propertyModelBuilder.isReadable() && !propertyModelBuilder.isWritable()) {
                propertiesToRemove.add(propertyModelBuilder.getName());
            }
        }
        for (String propertyName : propertiesToRemove) {
            classModelBuilder.removeProperty(propertyName);
        }
    }

    private <T> void processBsonExtraElementsAnnotation(final PropertyModelBuilder<T> propertyModelBuilder) {
        PropertyAccessor<T> propertyAccessor = propertyModelBuilder.getPropertyAccessor();
        if (!(propertyAccessor instanceof PropertyAccessorImpl)) {
            throw new CodecConfigurationException(format("The @BsonExtraElements annotation is not compatible with "
                            + "propertyModelBuilder instances that have custom implementations of org.bson.codecs.pojo.PropertyAccessor: %s",
                    propertyModelBuilder.getPropertyAccessor().getClass().getName()));
        }

        if (!Map.class.isAssignableFrom(propertyModelBuilder.getTypeData().getType())) {
            throw new CodecConfigurationException(format("The @BsonExtraElements annotation is not compatible with "
                            + "propertyModelBuilder with the following type: %s. "
                            + "Please use a Document, BsonDocument or Map<String, Object> type.",
                    propertyModelBuilder.getTypeData()));
        }
        propertyModelBuilder.propertySerialization(new PropertyModelSerializationInlineImpl<>(propertyModelBuilder.getPropertySerialization()));
        propertyModelBuilder.propertyAccessor(new FieldPropertyAccessor<>((PropertyAccessorImpl<T>) propertyAccessor));
    }
}
