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
import org.bson.codecs.configuration.CodecConfigurationException;

/**
 * A variant of {@link org.bson.codecs.configuration.CodecRegistry} that generates codecs for {@link PojoCodec}.
 *
 * <p>This is a specialized codec registry that retrieves codecs which account for type parameters associated with
 * a property. In particular this should only be used to add support for custom container types like optionals.
 * It's only applicable for use by {@link PojoCodec} registered through {@link PojoCodecProvider#builder()}.
 *
 * @since 3.6
 */
public interface PropertyCodecRegistry {

    /**
     * Gets a {@code Codec} for the given Class.
     *
     * @param type the Class associated type parameters for this property for which to get a Codec
     * @param <T> the class type
     * @return a codec for the given class
     * @throws CodecConfigurationException if the registry does not contain a codec for the given class.
     */
    <T> Codec<T> get(TypeWithTypeParameters<T> type);
}
