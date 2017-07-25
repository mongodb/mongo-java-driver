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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class DiscriminatorLookup {
    private final Map<String, Class<?>> discriminatorClassMap = new ConcurrentHashMap<String, Class<?>>();
    private final Set<String> packages;

    DiscriminatorLookup(final Map<Class<?>, ClassModel<?>> classModels, final Set<String> packages) {
        for (ClassModel<?> classModel : classModels.values()) {
            if (classModel.getDiscriminator() != null) {
                discriminatorClassMap.put(classModel.getDiscriminator(), classModel.getType());
            }
        }
        this.packages = packages;
    }

    Class<?> lookup(final String discriminator) {
        if (discriminatorClassMap.containsKey(discriminator)) {
            return discriminatorClassMap.get(discriminator);
        }

        Class<?> clazz = getClassForName(discriminator);
        if (clazz == null) {
            clazz = searchPackages(discriminator);
        }

        if (clazz == null) {
            throw new CodecConfigurationException("A class could not be found for: " + discriminator);
        } else {
            discriminatorClassMap.put(discriminator, clazz);
        }
        return clazz;
    }

    private Class<?> getClassForName(final String discriminator) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(discriminator);
        } catch (final ClassNotFoundException e) {
            // Ignore
        }
        return clazz;
    }

    private Class<?> searchPackages(final String discriminator) {
        Class<?> clazz = null;
        for (String packageName : packages) {
            clazz = getClassForName(packageName + "." + discriminator);
            if (clazz != null) {
                return clazz;
            }
        }
        return clazz;
    }
}
