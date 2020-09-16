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

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

final class ConstructorCreatorExecutable<T> implements CreatorExecutable<T> {
    private final Class<T> clazz;
    private final Constructor<T> constructor;
    private final List<String> properties = new ArrayList<>();
    private final Integer idPropertyIndex;
    private final List<Class<?>> parameterTypes = new ArrayList<Class<?>>();
    private final List<Type> parameterGenericTypes = new ArrayList<Type>();

    ConstructorCreatorExecutable(final Class<T> clazz, final Constructor<T> constructor) {
        this.clazz = clazz;
        this.constructor = constructor;
        Integer idPropertyIndex = null;

        if (constructor != null) {
            Class<?>[] paramTypes = constructor.getParameterTypes();
            Type[] genericParamTypes = constructor.getGenericParameterTypes();
            parameterTypes.addAll(asList(paramTypes));
            parameterGenericTypes.addAll(asList(genericParamTypes));

            Annotation[][] parameterAnnotations = constructor.getParameterAnnotations();
            idPropertyIndex = CreatorExecutable.initializeDataFromAnnotations(parameterAnnotations, properties);
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
    @SuppressWarnings("unchecked")
    public T getInstance(final Object... params) {
        checkHasAnExecutable();
        try {
            return constructor.newInstance(params);
        } catch (Exception e) {
            throw new CodecConfigurationException(e.getMessage(), e);
        }
    }

    @Override
    public CodecConfigurationException getError(final Class<?> clazz, final String msg) {
        return new CodecConfigurationException(format("Invalid @BsonCreator constructor in %s. %s",
                clazz.getSimpleName(), msg));    }

    private void checkHasAnExecutable() {
        if (constructor == null) {
            throw new CodecConfigurationException(format("Cannot find a public constructor for '%s'.", clazz.getSimpleName()));
        }
    }
}
