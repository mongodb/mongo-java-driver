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

package org.bson.codecs.configuration;

import org.bson.assertions.Assertions;
import org.bson.codecs.Codec;

import java.lang.reflect.Type;
import java.util.List;

/**
 * A registry of Codec instances searchable by the class that the Codec can encode and decode.
 *
 * <p>While the {@code CodecRegistry} interface adds no stipulations to the general contract for the Object.equals,
 * programmers who implement the {@code CodecRegistry} interface "directly" must exercise care if they choose to override the
 * {@code Object.equals}. It is not necessary to do so, and the simplest course of action is to rely on Object's implementation, but the
 * implementer may wish to implement a "value comparison" in place of the default "reference comparison."</p>
 *
 * <p>As of the 4.0 release, this class extends the {@code CodecProvider} interface.  This capability was introduced to enable nesting
 * registries inside another registry.</p>
 *
 * <p>Applications are encouraged to NOT implement this interface, but rather use the factory methods in {@link CodecRegistries}.</p>
 *
 * @since 3.0
 * @see CodecRegistries
 */
public interface CodecRegistry extends CodecProvider {
    /**
     * Gets a {@code Codec} for the given Class.
     *
     * @param clazz the class
     * @param <T> the class type
     * @return a codec for the given class
     * @throws CodecConfigurationException if the registry does not contain a codec for the given class.
     */
    <T> Codec<T> get(Class<T> clazz);

    /**
     * Gets a Codec for the given parameterized class, after resolving any type variables with the given type arguments.
     *
     * <p>
     * The default behavior is to throw a {@link AssertionError}, as it's expected that {@code CodecRegistry} implementations are always
     * provided by this library and will override the method appropriately.
     * </p>
     *
     * @param clazz         the parameterized class
     * @param typeArguments the type arguments to apply to the parameterized class.  This list may be empty but not null.
     * @param <T>           the class type
     * @return a codec for the given class, with the given type parameters resolved
     * @throws CodecConfigurationException if no codec can be found for the given class and type arguments.
     * @throws AssertionError              by default, if the implementation does not override this method, or if no codec can be found
     *                                     for the given class and type arguments.
     * @see org.bson.codecs.Parameterizable
     * @since 4.8
     */
    default <T> Codec<T> get(Class<T> clazz, List<Type> typeArguments) {
        throw Assertions.fail("This method should have been overridden but was not.");
    }
}
