/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.codecs.configuration;

import org.bson.codecs.Codec;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * A helper class for creating and combining codec registries
 *
 * @since 3.0
 */
public final class CodecRegistryHelper {

    /**
     * Creates a codec registry from the provided codec
     *
     * <p>This registry can then be used alongside other registries.  Typically used when adding extra codecs to existing codecs with the
     * {@link this#fromRegistries} helper.</p>
     *
     * @param codec the codec to create a registry for
     * @param <T> the value type of the codec
     * @return a codec registry for the given codec.
     */
    public static <T> CodecRegistry fromCodec(final Codec<T> codec) {
        return new SingleCodecRegistry<T>(codec);
    }

    /**
     *  A codec registry that uses a single codec provider when looking for codecs. {@see fromProviders}
     *
     * @param codecProvider the codec provider
     * @return a codec registry that uses a codec provider to find codecs
     */
    public static CodecRegistry fromProvider(final CodecProvider codecProvider) {
        return fromProviders(asList(codecProvider));
    }

    /**
     * A codec registry that contains a list of providers to use when looking for codecs.
     *
     * This class can handle cycles of Codec dependencies, i.e when the construction of a Codec for class A requires the construction of a
     * Codec for class B, and vice versa.
     *
     * @param codecProviders the list of codec providers
     * @return a codec registry that has an ordered list of codec providers.
     */
    public static CodecRegistry fromProviders(final List<CodecProvider> codecProviders) {
        return new ProvidersCodecRegistry(codecProviders);
    }

    /**
     * A codec registry that compounds two registries.
     *
     * <p>The first registry is checked first then if that returns null the second registry is checked.</p>
     *
     *
     * @param firstRegistry the preferred registry for codec lookups
     * @param secondRegistry the fallback registry for codec lookups.
     *
     * @return a codec registry that has a preferred registry when looking for codecs.
     */
    public static CodecRegistry fromRegistries(final CodecRegistry firstRegistry, final CodecRegistry secondRegistry) {
        return new CompoundCodecRegistry(firstRegistry, secondRegistry);
    }

    private CodecRegistryHelper() {
    }
}
