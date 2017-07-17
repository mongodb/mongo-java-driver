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


import org.bson.codecs.pojo.annotations.Creator;
import org.bson.codecs.pojo.annotations.Discriminator;
import org.bson.codecs.pojo.annotations.Id;
import org.bson.codecs.pojo.annotations.Property;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

final class ConventionAnnotationImpl implements Convention {

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        for (final Annotation annotation : classModelBuilder.getAnnotations()) {
            processClassAnnotation(classModelBuilder, annotation);
        }

        for (final PropertyModelBuilder<?> propertyModelBuilder : classModelBuilder.getPropertyModelBuilders()) {
            for (final Annotation annotation : propertyModelBuilder.getAnnotations()) {
                processPropertyAnnotation(classModelBuilder, propertyModelBuilder, annotation);
            }
        }

        processCreatorAnnotation(classModelBuilder);
    }

    @SuppressWarnings("unchecked")
    private <T> void processCreatorAnnotation(final ClassModelBuilder<T> classModelBuilder) {
        Class<T> clazz = classModelBuilder.getType();
        CreatorExecutable<T> creatorExecutable = null;
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            if (isPublic(constructor.getModifiers())) {
                for (Annotation annotation : constructor.getDeclaredAnnotations()) {
                    if (annotation.annotationType().equals(Creator.class)) {
                        creatorExecutable = new CreatorExecutable<T>(clazz, (Constructor<T>) constructor);
                        break;
                    }
                }
            }
        }

        if (creatorExecutable == null) {
            for (Method method : clazz.getDeclaredMethods()) {
                if (isStatic(method.getModifiers())) {
                    for (Annotation annotation : method.getDeclaredAnnotations()) {
                        if (annotation.annotationType().equals(Creator.class)) {
                            creatorExecutable = new CreatorExecutable<T>(clazz, method);
                            break;
                        }
                    }
                }
            }
        }

        if (creatorExecutable != null) {
            List<Property> properties = creatorExecutable.getProperties();
            List<Class<?>> parameterTypes = creatorExecutable.getParameterTypes();
            if (properties.size() != parameterTypes.size()) {
                throw creatorExecutable.getError("All parameters must be annotated with a @Property");
            }
            for (int i = 0; i < properties.size(); i++) {
                Property property = properties.get(i);
                Class<?> parameterType = parameterTypes.get(i);
                PropertyModelBuilder<?> propertyModelBuilder = classModelBuilder.getProperty(property.value());
                if (propertyModelBuilder == null) {
                    throw creatorExecutable.getError(format("Missing Property with the value: '%s'", property.value()));
                } else if (propertyModelBuilder.getTypeData().getType() != parameterType) {
                    throw creatorExecutable.getError(format("Invalid Property type for '%s'. Expected %s, found %s.", property.value(),
                            propertyModelBuilder.getTypeData().getType(), parameterType));
                }
            }
            classModelBuilder.instanceCreatorFactory(new InstanceCreatorFactoryImpl<T>(creatorExecutable));
        }
    }

    private void processClassAnnotation(final ClassModelBuilder<?> classModelBuilder, final Annotation annotation) {
        if (annotation instanceof Discriminator) {
            Discriminator discriminator = (Discriminator) annotation;
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

    private void processPropertyAnnotation(final ClassModelBuilder<?> classModelBuilder,
                                           final PropertyModelBuilder<?> propertyModelBuilder,
                                           final Annotation annotation) {
        if (annotation instanceof Property) {
            Property property = (Property) annotation;
            if (!"".equals(property.value())) {
                propertyModelBuilder.documentPropertyName(property.value());
            }
            propertyModelBuilder.discriminatorEnabled(property.useDiscriminator());
        } else if (annotation instanceof Id) {
            classModelBuilder.idPropertyName(propertyModelBuilder.getPropertyName());
        }
    }
}
