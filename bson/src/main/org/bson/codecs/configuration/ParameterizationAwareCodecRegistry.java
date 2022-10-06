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

import org.bson.codecs.Codec;

import java.lang.reflect.Type;
import java.util.List;

/**
 * A {@link CodecRegistry} that supports registration of parameterized types.
 *
 * @since 4.8
 */
public interface ParameterizationAwareCodecRegistry extends CodecRegistry {
    /**
     * Gets a Codec for the given parameterized class, after resolving any type variables with the given type arguments.
     *
     * @param clazz         the parameterized class
     * @param typeArguments the type arguments to apply to the parameterized class
     * @param <T>           the class type
     * @return a codec for the given class, with the given type parameters resolved
     * @throws CodecConfigurationException if no codec can be found for the given class and type arguments.
     */
    <T> Codec<T> get(Class<T> clazz, List<Type> typeArguments);
}
