/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
package org.bson.codecs.configuration.mapper;

import com.fasterxml.classmate.TypeResolver;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.mapper.conventions.ConventionPack;
import org.bson.codecs.configuration.mapper.conventions.DefaultConventionPack;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Provides Codecs for various POJOs via the ClassModel abstractions.
 */
public class ClassModelCodecProvider implements CodecProvider {
    private final TypeResolver resolver = new TypeResolver();
    private final Set<Class<?>> registered;
    private final Map<Class<?>, ClassModelCodec<?>> mapped = new HashMap<Class<?>, ClassModelCodec<?>>();
    private final ConventionPack conventionPack;

    /**
     * Creates a provider for a given set of classes.
     *
     * @param conventionPack The conventions to use when mapping
     * @param registered     the classes to use
     */
    public ClassModelCodecProvider(final ConventionPack conventionPack, final Set<Class<?>> registered) {
        this.conventionPack = conventionPack;
        this.registered = registered;
    }

    /**
     * Creates a Builder so classes can be registered before creating an immutable Provider.
     *
     * @return the Builder
     * @see ProviderBuilder#register(Class)
     */
    public static ProviderBuilder builder() {
        return new ProviderBuilder();
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        ClassModelCodec<?> codec = null;
        if (registered.contains(clazz)) {
            codec = mapped.get(clazz);
            if (codec == null && conventionPack.isMappable(clazz)) {
                final ClassModel model = new ClassModel(registry, resolver, clazz);
                conventionPack.apply(model);
                codec = new ClassModelCodec(model);
                mapped.put(clazz, codec);
            }
        }
        return (Codec<T>) codec;
    }

    /**
     * A Builder for the ClassModelCodecProvider
     */
    public static class ProviderBuilder {
        private final Set<Class<?>> registered = new HashSet<Class<?>>();
        private ConventionPack conventionPack = new DefaultConventionPack();

        /**
         * Creates the ClassModelCodecProvider with the classes that have been registered.
         *
         * @return the Provider
         * @see #register(Class)
         */
        public ClassModelCodecProvider build() {
            return new ClassModelCodecProvider(conventionPack, registered);
        }

        /**
         * Registers a class with the builder for inclusion in the Provider.
         *
         * @param clazz the class to register
         * @return this
         */
        public ProviderBuilder register(final Class<?> clazz) {
            registered.add(clazz);
            return this;
        }

        /**
         * Sets the conventions to use when mapping types.
         *
         * @param conventionPack the ConventionPack to use with the ClassModelCodecProvider
         */
        public void setConventionPack(final ConventionPack conventionPack) {
            this.conventionPack = conventionPack;
        }
    }
}
