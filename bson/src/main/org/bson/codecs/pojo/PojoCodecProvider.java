/*
 * Copyright 2017 MongoDB, Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.bson.assertions.Assertions.notNull;

/**
 * Provides Codecs for registered POJOs via the ClassModel abstractions.
 *
 * @since 3.5
 */
public final class PojoCodecProvider implements CodecProvider {
    private final Map<Class<?>, ClassModel<?>> classModels;
    private final Set<String> packages;
    private final List<Convention> conventions;
    private final DiscriminatorLookup discriminatorLookup;

    private PojoCodecProvider(final Map<Class<?>, ClassModel<?>> classModels, final Set<String> packages,
                              final List<Convention> conventions) {
        this.classModels = classModels;
        this.packages = packages;
        this.conventions = conventions;
        this.discriminatorLookup = new DiscriminatorLookup(classModels, packages);
    }

    /**
     * Creates a Builder so classes or packages can be registered and configured before creating an immutable CodecProvider.
     *
     * @return the Builder
     * @see Builder#register(Class[])
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        return getPojoCodec(clazz, registry);
    }

    @SuppressWarnings("unchecked")
    <T> PojoCodec<T> getPojoCodec(final Class<T> clazz, final CodecRegistry registry) {
        ClassModel<T> classModel = (ClassModel<T>) classModels.get(clazz);
        if (classModel != null || (clazz.getPackage() != null && packages.contains(clazz.getPackage().getName()))) {
            if (classModel == null) {
                classModel = createClassModel(clazz, conventions);
            }
            return new PojoCodec<T>(classModel, this, registry, discriminatorLookup);
        }
        return null;
    }

    /**
     * A Builder for the PojoCodecProvider
     */
    public static final class Builder {
        private final Set<String> packages = new HashSet<String>();
        private final Map<Class<?>, ClassModel<?>> classModels = new HashMap<Class<?>, ClassModel<?>>();
        private final List<Class<?>> clazzes = new ArrayList<Class<?>>();
        private List<Convention> conventions = null;

        /**
         * Creates the PojoCodecProvider with the classes or packages that configured and registered.
         *
         * @return the Provider
         * @see #register(Class...)
         */
        public PojoCodecProvider build() {
            List<Convention> immutableConventions = conventions != null
                    ? Collections.unmodifiableList(new ArrayList<Convention>(conventions))
                    : null;
            for (Class<?> clazz : clazzes) {
                if (!classModels.containsKey(clazz)) {
                    register(createClassModel(clazz, immutableConventions));
                }
            }
            return new PojoCodecProvider(classModels, packages, immutableConventions);
        }

        /**
         * Sets the conventions to use when creating {@code ClassModels} from classes or packages.
         *
         * @param conventions a list of conventions
         * @return this
         */
        public Builder conventions(final List<Convention> conventions) {
            this.conventions = notNull("conventions", conventions);
            return this;
        }

        /**
         * Registers a classes with the builder for inclusion in the Provider.
         *
         * <p>Note: Uses reflection for the property mapping. If no conventions are configured on the builder the
         * {@link Conventions#DEFAULT_CONVENTIONS} will be used.</p>
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
         * Registers the packages of the given classes with the builder for inclusion in the Provider. This will allow classes in the
         * given packages to mapped for use with PojoCodecProvider.
         *
         * <p>Note: Uses reflection for the field mapping. If no conventions are configured on the builder the
         * {@link Conventions#DEFAULT_CONVENTIONS} will be used.</p>
         *
         * @param packageNames the package names to register
         * @return this
         */
        public Builder register(final String... packageNames) {
            notNull("packageNames", packageNames);
            for (String name : packageNames) {
                packages.add(name);
            }
            return this;
        }

        private Builder() {
        }
    }

    private static <T> ClassModel<T> createClassModel(final Class<T> clazz, final List<Convention> conventions) {
        ClassModelBuilder<T> builder = ClassModel.builder(clazz);
        if (conventions != null) {
            builder.conventions(conventions);
        }
        return builder.build();
    }

}
