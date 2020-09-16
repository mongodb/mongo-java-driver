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
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

final class RecordCreatorExecutable<T> implements CreatorExecutable<T> {
    private final Class<T> clazz;
    private final Constructor<T> constructor;
    private final List<String> properties = new ArrayList<>();
    private final Integer idPropertyIndex;
    private final List<Class<?>> parameterTypes = new ArrayList<>();
    private final List<Type> parameterGenericTypes = new ArrayList<>();

    RecordCreatorExecutable(final Class<T> clazz, final Constructor<T> constructor) {
        this.clazz = clazz;
        this.constructor = constructor;
        Integer idPropertyIndex = null;
        parameterTypes.addAll(asList(constructor.getParameterTypes()));
        parameterGenericTypes.addAll(asList(constructor.getGenericParameterTypes()));

        RecordComponent[] recordComponents = clazz.getRecordComponents();
        for (int i = 0; i < recordComponents.length; i++) {
            RecordComponent recordComponent = recordComponents[i];
            String name = recordComponent.getName();

            boolean isId = name.equals("id") || name.equals("_id");
            for (Annotation a : recordComponent.getAnnotations()) {
                if (a instanceof BsonId) {
                    isId = true;
                }
                else if (a instanceof BsonProperty) {
                    isId = ((BsonProperty) a).value().equals("_id");
                }
            }

            if (isId) {
                properties.add(null);
                idPropertyIndex = i;
            } else {
                properties.add(name);
            }
        }
        this.idPropertyIndex = idPropertyIndex;
    }

    @Override
    public Class<T> getType() {
        return clazz;
    }

    @Override
    public List<String> getProperties() {
        return properties;
    }

    @Override
    public Integer getIdPropertyIndex() {
        return idPropertyIndex;
    }

    @Override
    public List<Class<?>> getParameterTypes() {
        return parameterTypes;
    }

    @Override
    public List<Type> getParameterGenericTypes() {
        return parameterGenericTypes;
    }

    @Override
    public CodecConfigurationException getError(final Class<?> clazz, final String msg) {
        return new CodecConfigurationException(format("Invalid @BsonCrator constructor in %s. %s", clazz.getSimpleName(), msg));
    }

    @Override
    public T getInstance(final Object... params) {
        try {
            return constructor.newInstance(params);
        } catch (Exception e) {
            throw new CodecConfigurationException(e.getMessage(), e);
        }
    }
}
