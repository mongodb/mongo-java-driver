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

/**
 * A registry of Codec instances searchable by the class that the Codec can encode and decode.
 *
 * <p>While the {@code CodecRegistry} interface adds no stipulations to the general contract for the Object.equals,
 * programmers who implement the {@code CodecRegistry} interface "directly" must exercise care if they choose to override the
 * {@code Object.equals}. It is not necessary to do so, and the simplest course of action is to rely on Object's implementation, but the
 * implementer may wish to implement a "value comparison" in place of the default "reference comparison."</p>
 *
 * @since 3.0
 */
public interface CodecRegistry {
    /**
     * Gets a {@code Codec} for the given Class.
     *
     * @param clazz the class
     * @param <T> the class type
     * @return a codec for the given class
     * @throws CodecConfigurationException if the registry does not contain a codec for the given class.
     */
    <T> Codec<T> get(Class<T> clazz);
}
