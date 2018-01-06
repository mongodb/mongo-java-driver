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

import static java.lang.String.format;

final class PropertyAccessorImpl<T> implements PropertyAccessor<T> {

    private final PropertyMetadata<T> propertyMetadata;

    PropertyAccessorImpl(final PropertyMetadata<T> propertyMetadata) {
        this.propertyMetadata = propertyMetadata;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S> T get(final S instance) {
        try {
            if (propertyMetadata.isSerializable()) {
                if (propertyMetadata.getGetter() != null) {
                    return (T) propertyMetadata.getGetter().invoke(instance);
                } else {
                    return (T) propertyMetadata.getField().get(instance);
                }
            } else {
                throw getError(null);
            }
        } catch (final Exception e) {
            throw getError(e);
        }
    }

    @Override
    public <S> void set(final S instance, final T value) {
        try {
            if (propertyMetadata.isDeserializable()) {
                if (propertyMetadata.getSetter() != null) {
                    propertyMetadata.getSetter().invoke(instance, value);
                } else {
                    propertyMetadata.getField().set(instance, value);
                }
            }
        } catch (final Exception e) {
            throw setError(e);
        }
    }

    PropertyMetadata<T> getPropertyMetadata() {
        return propertyMetadata;
    }

    private CodecConfigurationException getError(final Exception cause) {
        return new CodecConfigurationException(format("Unable to get value for property '%s' in %s", propertyMetadata.getName(),
                propertyMetadata.getDeclaringClassName()), cause);
    }

    private CodecConfigurationException setError(final Exception cause) {
        return new CodecConfigurationException(format("Unable to set value for property '%s' in %s", propertyMetadata.getName(),
                propertyMetadata.getDeclaringClassName()), cause);
    }
}
