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
import org.bson.codecs.pojo.annotations.BsonId;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;

final class CreatorExecutable<T> {
    private final Class<T> clazz;
    private final Constructor<T> constructor;
    private final Method method;
    private final List<BsonProperty> properties = new ArrayList<BsonProperty>();
    private final Integer idPropertyIndex;
    private final List<Class<?>> parameterTypes = new ArrayList<Class<?>>();
    private final List<Type> parameterGenericTypes = new ArrayList<Type>();

    CreatorExecutable(final Class<T> clazz, final Constructor<T> constructor) {
        this(clazz, constructor, null);
    }

    CreatorExecutable(final Class<T> clazz, final Method method) {
        this(clazz, null, method);
    }

    private CreatorExecutable(final Class<T> clazz, final Constructor<T> constructor, final Method method) {
        this.clazz = clazz;
        this.constructor = constructor;
        this.method = method;
        Integer idPropertyIndex = null;

        if (constructor != null || method != null) {
            Class<?>[] paramTypes = constructor != null ? constructor.getParameterTypes() : method.getParameterTypes();
            Type[] genericParamTypes = constructor != null ? constructor.getGenericParameterTypes() : method.getGenericParameterTypes();
            parameterTypes.addAll(asList(paramTypes));
            parameterGenericTypes.addAll(asList(genericParamTypes));
            Annotation[][] parameterAnnotations = constructor != null ? constructor.getParameterAnnotations()
                    : method.getParameterAnnotations();

            for (int i = 0; i < parameterAnnotations.length; ++i) {
                Annotation[] parameterAnnotation = parameterAnnotations[i];

                for (Annotation annotation : parameterAnnotation) {
                    if (annotation.annotationType().equals(BsonProperty.class)) {
                        properties.add((BsonProperty) annotation);
                        break;
                    }

                    if (annotation.annotationType().equals(BsonId.class)) {
                        properties.add(null);
                        idPropertyIndex = i;
                        break;
                    }
                }
            }
        }

        this.idPropertyIndex = idPropertyIndex;
    }

    Class<T> getType() {
        return clazz;
    }

    List<BsonProperty> getProperties() {
        return properties;
    }

    Integer getIdPropertyIndex() {
        return idPropertyIndex;
    }

    List<Class<?>> getParameterTypes() {
        return parameterTypes;
    }

    List<Type> getParameterGenericTypes() {
        return parameterGenericTypes;
    }

    @SuppressWarnings("unchecked")
    T getInstance() {
        checkHasAnExecutable();
        try {
            if (constructor != null) {
                return constructor.newInstance();
            } else {
                return (T) method.invoke(clazz);
            }
        } catch (Exception e) {
            throw new CodecConfigurationException(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    T getInstance(final Object[] params) {
        checkHasAnExecutable();
        try {
            if (constructor != null) {
                return constructor.newInstance(params);
            } else {
                return (T) method.invoke(clazz, params);
            }
        } catch (Exception e) {
            throw new CodecConfigurationException(e.getMessage(), e);
        }
    }


    CodecConfigurationException getError(final Class<?> clazz, final String msg) {
        return getError(clazz, constructor != null, msg);
    }

    private void checkHasAnExecutable() {
        if (constructor == null && method == null) {
            throw new CodecConfigurationException(format("Cannot find a public constructor for '%s'.", clazz.getSimpleName()));
        }
    }

    private static CodecConfigurationException getError(final Class<?> clazz, final boolean isConstructor, final String msg) {
        return new CodecConfigurationException(format("Invalid @BsonCreator %s in %s. %s", isConstructor ? "constructor" : "method",
                clazz.getSimpleName(), msg));
    }

}
