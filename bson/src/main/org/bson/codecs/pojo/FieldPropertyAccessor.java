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

import org.bson.codecs.configuration.CodecConfigurationException;

import static java.lang.String.format;

final class FieldPropertyAccessor<T> implements PropertyAccessor<T> {
    private final PropertyAccessorImpl<T> wrapped;

    FieldPropertyAccessor(final PropertyAccessorImpl<T> wrapped) {
        this.wrapped = wrapped;
        try {
            wrapped.getPropertyMetadata().getField().setAccessible(true);
        } catch (Exception e) {
            throw new CodecConfigurationException(format("Unable to make field accessible '%s' in %s",
                    wrapped.getPropertyMetadata().getName(), wrapped.getPropertyMetadata().getDeclaringClassName()), e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S> T get(final S instance) {
        try {
            return (T) wrapped.getPropertyMetadata().getField().get(instance);
        } catch (Exception e) {
            throw new CodecConfigurationException(format("Unable to get value for property '%s' in %s",
                    wrapped.getPropertyMetadata().getName(), wrapped.getPropertyMetadata().getDeclaringClassName()), e);
        }
    }

    @Override
    public <S> void set(final S instance, final T value) {
        try {
            wrapped.getPropertyMetadata().getField().set(instance, value);
        } catch (Exception e) {
            throw new CodecConfigurationException(format("Unable to set value for property '%s' in %s",
                    wrapped.getPropertyMetadata().getName(), wrapped.getPropertyMetadata().getDeclaringClassName()), e);
        }
    }
}
