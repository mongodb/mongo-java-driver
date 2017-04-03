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

import java.lang.reflect.Field;

import static java.lang.String.format;

final class FieldAccessorImpl<T> implements FieldAccessor<T> {
    private final Field field;
    private final String fieldName;

    FieldAccessorImpl(final Field field, final String fieldName) {
        this.field = field;
        this.fieldName = fieldName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <S> T get(final S instance) {
        try {
            return (T) field.get(instance);
        } catch (final IllegalAccessException e) {
            throw new CodecConfigurationException(format("Unable to get value for field: %s", fieldName), e);
        } catch (final IllegalArgumentException e) {
            throw new CodecConfigurationException(format("Unable to get value for field: %s", fieldName), e);
        }
    }

    @Override
    public <S> void set(final S instance, final T value) {
        try {
            field.set(instance, value);
        } catch (final IllegalAccessException e) {
            throw new CodecConfigurationException(format("Unable to set value '%s' for field: %s", value, fieldName), e);
        } catch (final IllegalArgumentException e) {
            throw new CodecConfigurationException(format("Unable to set value '%s' for field: %s", value, fieldName), e);
        }
    }
}
