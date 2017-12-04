/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.pojo;

import org.bson.codecs.configuration.CodecConfigurationException;

import java.lang.reflect.Field;

import static java.lang.String.format;
import static java.lang.reflect.Modifier.isPrivate;

final class ConventionSetPrivateFieldImpl implements Convention {

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        for (PropertyModelBuilder<?> propertyModelBuilder : classModelBuilder.getPropertyModelBuilders()) {
            if (propertyModelBuilder.getPropertyAccessor() instanceof PropertyAccessorImpl) {
                PropertyAccessorImpl<?> defaultAccessor = (PropertyAccessorImpl<?>) propertyModelBuilder.getPropertyAccessor();
                PropertyMetadata<?> propertyMetaData = defaultAccessor.getPropertyMetadata();
                if (!propertyMetaData.isDeserializable() && isPrivate(propertyMetaData.getField().getModifiers())) {
                    setPropertyAccessor(propertyModelBuilder);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private <T> void setPropertyAccessor(final PropertyModelBuilder<T> propertyModelBuilder) {
        propertyModelBuilder.propertyAccessor(new PrivateProperyAccessor<T>(
                (PropertyAccessorImpl<T>) propertyModelBuilder.getPropertyAccessor()));
    }

    private static final class PrivateProperyAccessor<T> implements PropertyAccessor<T> {
        private final PropertyAccessorImpl<T> wrapped;

        private PrivateProperyAccessor(final PropertyAccessorImpl<T> wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public <S> T get(final S instance) {
            return wrapped.get(instance);
        }

        @Override
        public <S> void set(final S instance, final T value) {
            try {
                Field field = wrapped.getPropertyMetadata().getField();
                field.setAccessible(true);
                field.set(instance, value);
            } catch (Exception e) {
                throw new CodecConfigurationException(format("Unable to set value for property '%s' in %s",
                        wrapped.getPropertyMetadata().getName(),
                        wrapped.getPropertyMetadata().getDeclaringClassName()), e);
            }
        }
    }
}
