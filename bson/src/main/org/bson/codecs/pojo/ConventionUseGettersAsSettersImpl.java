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

import java.util.Collection;
import java.util.Map;

import static java.lang.String.format;

final class ConventionUseGettersAsSettersImpl implements Convention {

    @Override
    public void apply(final ClassModelBuilder<?> classModelBuilder) {
        for (PropertyModelBuilder<?> propertyModelBuilder : classModelBuilder.getPropertyModelBuilders()) {
            if (!(propertyModelBuilder.getPropertyAccessor() instanceof PropertyAccessorImpl)) {
                throw new CodecConfigurationException(format("The USE_GETTER_AS_SETTER_CONVENTION is not compatible with "
                        + "propertyModelBuilder instance that have custom implementations of org.bson.codecs.pojo.PropertyAccessor: %s",
                        propertyModelBuilder.getPropertyAccessor().getClass().getName()));
            }
            PropertyAccessorImpl<?> defaultAccessor = (PropertyAccessorImpl<?>) propertyModelBuilder.getPropertyAccessor();
            PropertyMetadata<?> propertyMetaData = defaultAccessor.getPropertyMetadata();
            if (!propertyMetaData.isDeserializable() && propertyMetaData.isSerializable()
                    && isMapOrCollection(propertyMetaData.getTypeData().getType())) {
                setPropertyAccessor(propertyModelBuilder);
            }
        }
    }

    private <T> boolean isMapOrCollection(final Class<T> clazz) {
        return Collection.class.isAssignableFrom(clazz) || Map.class.isAssignableFrom(clazz);
    }

    @SuppressWarnings("unchecked")
    private <T> void setPropertyAccessor(final PropertyModelBuilder<T> propertyModelBuilder) {
        propertyModelBuilder.propertyAccessor(new PrivateProperyAccessor<T>(
                (PropertyAccessorImpl<T>) propertyModelBuilder.getPropertyAccessor()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
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
            if (value instanceof Collection) {
                mutateCollection(instance, (Collection) value);
            } else if (value instanceof Map) {
                mutateMap(instance, (Map) value);
            } else {
                throwCodecConfigurationException(format("Unexpected type: '%s'", value.getClass()), null);
            }
        }

        private <S> void mutateCollection(final S instance, final Collection value) {
            T originalCollection = get(instance);
            Collection<?> collection = ((Collection<?>) originalCollection);
            if (collection == null) {
                throwCodecConfigurationException("The getter returned null.", null);
            } else if (!collection.isEmpty()) {
                throwCodecConfigurationException("The getter returned a non empty collection.", null);
            } else {
                try {
                    collection.addAll(value);
                } catch (Exception e) {
                    throwCodecConfigurationException("collection#addAll failed.", e);
                }
            }
        }

        private <S> void mutateMap(final S instance, final Map value) {
            T originalMap = get(instance);
            Map<?, ?> map = ((Map<?, ?>) originalMap);
            if (map == null) {
                throwCodecConfigurationException("The getter returned null.", null);
            } else if (!map.isEmpty()) {
                throwCodecConfigurationException("The getter returned a non empty map.", null);
            } else {
                try {
                    map.putAll(value);
                } catch (Exception e) {
                    throwCodecConfigurationException("map#putAll failed.", e);
                }
            }
        }
        private void throwCodecConfigurationException(final String reason, final Exception cause) {
            throw new CodecConfigurationException(format("Cannot use getter in '%s' to set '%s'. %s",
                    wrapped.getPropertyMetadata().getDeclaringClassName(), wrapped.getPropertyMetadata().getName(), reason), cause);
        }
    }
}
