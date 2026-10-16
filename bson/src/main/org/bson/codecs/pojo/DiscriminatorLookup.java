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
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import static java.lang.String.format;

final class DiscriminatorLookup {
    private static final Logger LOGGER = Loggers.getLogger("PojoCodec");
    private final Map<String, Class<?>> discriminatorClassMap = new ConcurrentHashMap<>();
    private final Set<String> packages;

    DiscriminatorLookup(final Map<Class<?>, ClassModel<?>> classModels, final Set<String> packages) {
        for (ClassModel<?> classModel : classModels.values()) {
            if (classModel.getDiscriminator() != null) {
                discriminatorClassMap.put(classModel.getDiscriminator(), classModel.getType());
            }
        }
        this.packages = packages;
    }

    public Class<?> lookup(final Class<?> expectedType, final String discriminator) {
        Class<?> clazz = discriminatorClassMap.get(discriminator);
        if (clazz == null) {
            clazz = getClassForName(expectedType, discriminator);
            if (clazz == null) {
                clazz = searchPackages(expectedType, discriminator);
            }
            if (clazz == null) {
                throw new CodecConfigurationException(format("A class could not be found for the discriminator: '%s'.", discriminator));
            }
            discriminatorClassMap.put(discriminator, clazz);
        } else if (!expectedType.isAssignableFrom(clazz)) {
            logNotAssignable(clazz, expectedType);
            throw new CodecConfigurationException(format("The discriminator '%s' resolved to '%s', which is not assignable to '%s'.",
                    discriminator, clazz.getName(), expectedType.getName()));
        }
        return clazz;
    }

    void addClassModel(final ClassModel<?> classModel) {
        if (classModel.getDiscriminator() != null) {
            discriminatorClassMap.put(classModel.getDiscriminator(), classModel.getType());
        }
    }

    /**
     * Loads the class without initializing it, so that static initializers of classes that are not assignable to the expected type
     * never run.
     */
    @Nullable
    private static Class<?> getClassForName(final Class<?> expectedType, final String className) {
        try {
            boolean init = false;
            Class<?> clazz = Class.forName(className, init, DiscriminatorLookup.class.getClassLoader());
            if (expectedType.isAssignableFrom(clazz)) {
                return clazz;
            }
            logNotAssignable(clazz, expectedType);
            return null;
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static void logNotAssignable(final Class<?> clazz, final Class<?> expectedType) {
        LOGGER.warn(format("Ignoring discriminator class '%s' as it is not assignable to '%s'.",
                clazz.getName(), expectedType.getName()));
    }

    @Nullable
    private Class<?> searchPackages(final Class<?> expectedType, final String discriminator) {
        for (String packageName : packages) {
            Class<?> clazz = getClassForName(expectedType, packageName + "." + discriminator);
            if (clazz != null) {
                return clazz;
            }
        }
        return null;
    }
}
