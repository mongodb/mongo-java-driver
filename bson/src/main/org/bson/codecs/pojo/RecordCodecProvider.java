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

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.bson.assertions.Assertions.notNull;

/**
 * Provides Codecs for registered Records via the ClassModel abstractions.
 *
 * @since TODO
 */
public final class RecordCodecProvider implements CodecProvider {
    static final Logger LOGGER = Loggers.getLogger("codecs.record");
    private final boolean automatic;
    private final Map<Class<?>, ClassModel<?>> classModels;
    private final Set<String> packages;
    private final DiscriminatorLookup discriminatorLookup;
    private final List<PropertyCodecProvider> propertyCodecProviders;

    private RecordCodecProvider(final boolean automatic, final Map<Class<?>, ClassModel<?>> classModels, final Set<String> packages,
                              final List<PropertyCodecProvider> propertyCodecProviders) {
        this.automatic = automatic;
        this.classModels = classModels;
        this.packages = packages;
        this.discriminatorLookup = new DiscriminatorLookup(classModels, packages);
        this.propertyCodecProviders = propertyCodecProviders;
    }

    /**
     * Creates a Builder so classes or packages can be registered and configured before creating an immutable CodecProvider.
     *
     * @return the Builder
     * @see RecordCodecProvider.Builder#register(Class[])
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (!PojoBuilderHelper.isRecord(clazz)) {
            return null;
        }

        ClassModel<T> classModel = (ClassModel<T>) classModels.get(clazz);
        if (classModel != null) {
            return new PojoCodecImpl<T>(classModel, registry, propertyCodecProviders, discriminatorLookup);
        } else if (automatic || (clazz.getPackage() != null && packages.contains(clazz.getPackage().getName()))) {
            try {
                classModel = createClassModel(clazz);
                if (clazz.isInterface() || !classModel.getPropertyModels().isEmpty()) {
                    discriminatorLookup.addClassModel(classModel);
                    return new AutomaticPojoCodec<T>(new PojoCodecImpl<T>(classModel, registry, propertyCodecProviders,
                            discriminatorLookup));
                }
            } catch (Exception e) {
                LOGGER.warn(format("Cannot use '%s' with the PojoCodec for records.", clazz.getSimpleName()), e);
                return null;
            }
        }
        return null;
    }

    /**
     * A Builder for the RecordCodecProvider
     */
    public static final class Builder {
        private final Set<String> packages = new HashSet<String>();
        private final Map<Class<?>, ClassModel<?>> classModels = new HashMap<Class<?>, ClassModel<?>>();
        private final List<Class<?>> clazzes = new ArrayList<Class<?>>();
        private List<Convention> conventions = null;
        private final List<PropertyCodecProvider> propertyCodecProviders = new ArrayList<PropertyCodecProvider>();
        private boolean automatic;

        /**
         * Sets whether the provider should automatically try to wrap a {@link ClassModel} for any class that is requested.
         *
         * @param automatic whether to automatically wrap {@code ClassModels} or not.
         * @return this
         */
        public Builder automatic(final boolean automatic) {
            this.automatic = automatic;
            return this;
        }

        /**
         * Registers a classes with the builder for inclusion in the Provider.
         *
         * <p>Note: Uses reflection for the property mapping.
         *
         * @param classes the classes to register
         * @return this
         */
        public Builder register(final Class<?>... classes) {
            clazzes.addAll(asList(classes));
            return this;
        }

        /**
         * Registers classModels for inclusion in the Provider.
         *
         * @param classModels the classModels to register
         * @return this
         */
        public Builder register(final ClassModel<?>... classModels) {
            notNull("classModels", classModels);
            for (ClassModel<?> classModel : classModels) {
                this.classModels.put(classModel.getType(), classModel);
            }
            return this;
        }

        /**
         * Registers the packages of the given classes with the builder for inclusion in the Provider. This will allow records in the
         * given packages to mapped for use with RecordCodecProvider.
         *
         * <p>Note: Uses reflection for the field mapping.
         *
         * @param packageNames the package names to register
         * @return this
         */
        public Builder register(final String... packageNames) {
            packages.addAll(asList(notNull("packageNames", packageNames)));
            return this;
        }

        /**
         * Registers codec providers that receive the type parameters of properties for instances encoded and decoded
         * by a {@link PojoCodec} for records, handled by this provider.
         *
         * <p>Note that you should prefer working with the {@link CodecRegistry}/{@link CodecProvider} hierarchy. Providers
         * should only be registered here if a codec needs to be created for custom container types like optionals and
         * collections. Support for types {@link Map} and {@link java.util.Collection} are built-in so explicitly handling
         * them is not necessary.
         * @param providers property codec providers to register
         * @return this
         */
        public Builder register(final PropertyCodecProvider... providers) {
            propertyCodecProviders.addAll(asList(notNull("providers", providers)));
            return this;
        }

        private Builder() {}

        /**
         * Creates the RecordCodecProvider for records with the classes or packages that configured and registered.
         *
         * @return the Provider
         * @see #register(Class...)
         */
        public RecordCodecProvider build() {
            for (Class<?> clazz : clazzes) {
                if (!classModels.containsKey(clazz)) {
                    register(createClassModel(clazz));
                }
            }
            return new RecordCodecProvider(automatic, classModels, packages, propertyCodecProviders);
        }
    }

    private static <T> ClassModel<T> createClassModel(final Class<T> clazz) {
        return ClassModel.builder(clazz).build();
    }
}
