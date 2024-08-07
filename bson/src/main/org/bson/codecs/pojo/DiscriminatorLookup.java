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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

final class DiscriminatorLookup {
    private static final Logger LOGGER = Loggers.getLogger("PojoCodec");
    private final Map<Discriminator, Class<?>> discriminatorClassMap = new ConcurrentHashMap<>();
    private final Set<String> packages;

    DiscriminatorLookup(final Map<Class<?>, ClassModel<?>> classModels, final Set<String> packages) {
        for (ClassModel<?> classModel : classModels.values()) {
            addClassModel(classModel);
        }
        this.packages = packages;
    }

    public Class<?> lookup(final String discriminatorKey, final String discriminatorValue) {
        Discriminator discriminator = new Discriminator(discriminatorKey, discriminatorValue);

        if (discriminatorClassMap.containsKey(discriminator)) {
            return discriminatorClassMap.get(discriminator);
        }

        Class<?> clazz = getClassForName(discriminatorValue);
        if (clazz == null) {
            clazz = searchPackages(discriminatorValue);
        }

        if (clazz == null) {
            throw new CodecConfigurationException(format("A class could not be found for the discriminator: '%s'.",  discriminator));
        } else {
            discriminatorClassMap.put(discriminator, clazz);
        }
        return clazz;
    }

    void addClassModel(final ClassModel<?> classModel) {
        if (classModel.getDiscriminator() != null) {
            Discriminator discriminator = Discriminator.create(classModel);
            Class<?> cachedClass = discriminatorClassMap.computeIfAbsent(discriminator, (cachedDiscriminator) -> classModel.getType());
            if (cachedClass != classModel.getType()) {
                LOGGER.error(format("Duplicate %s registered for both: `%s` and `%s`",
                        discriminator, cachedClass.getName(), classModel.getType().getName()));
            }
        }
    }

    private Class<?> getClassForName(final String discriminator) {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(discriminator);
        } catch (ClassNotFoundException e) {
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

    static final class Discriminator {
        private final String key;
        private final String value;

        static Discriminator create(final ClassModel<?> classModel) {
            return new Discriminator(classModel.getDiscriminatorKey(), classModel.getDiscriminator());
        }

        Discriminator(final String key, final String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return "Discriminator{"
                    + "key='" + key + '\''
                    + ", value='" + value + '\''
                    + '}';
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Discriminator that = (Discriminator) o;
            return Objects.equals(key, that.key) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }
    }
}
