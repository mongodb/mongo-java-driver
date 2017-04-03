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


import org.bson.codecs.pojo.annotations.Discriminator;
import org.bson.codecs.pojo.annotations.Id;
import org.bson.codecs.pojo.annotations.Property;

import java.lang.annotation.Annotation;

final class ConventionAnnotationImpl implements Convention {

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        for (final Annotation annotation : classModelBuilder.getAnnotations()) {
            processClassAnnotation(classModelBuilder, annotation);
        }

        for (final FieldModelBuilder<?> fieldModelBuilder : classModelBuilder.getFields()) {
            for (final Annotation annotation : fieldModelBuilder.getAnnotations()) {
                processFieldAnnotation(classModelBuilder, fieldModelBuilder, annotation);
            }
        }
    }

    void processClassAnnotation(final ClassModelBuilder<?> classModelBuilder, final Annotation annotation) {
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
            classModelBuilder.discriminatorEnabled(true);
        }
    }

    void processFieldAnnotation(final ClassModelBuilder<?> classModelBuilder, final FieldModelBuilder<?> fieldModelBuilder,
                                          final Annotation annotation) {
        if (annotation instanceof Property) {
            Property property = (Property) annotation;
            if (!"".equals(property.name())) {
                fieldModelBuilder.documentFieldName(property.name());
            }
            fieldModelBuilder.discriminatorEnabled(property.useDiscriminator());
        } else if (annotation instanceof Id) {
            classModelBuilder.idField(fieldModelBuilder.getFieldName());
        }
    }

}
