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

package org.bson.codecs;

import org.bson.BsonType;
import org.bson.codecs.configuration.CodecConfigurationException;

/**
 * Implementations of this interface can decode additional types
 * and translate them to the desired value type depending on the BsonRepresentation.
 *
 * @param <T> the value type
 * @since 4.2
 */
public interface RepresentationConfigurable<T> {

    /**
     * Gets the BsonRepresentation.
     *
     * @return the BsonRepresentation
     */
    BsonType getRepresentation();

    /**
     * Returns an immutable codec with the given representation. If the provided representation
     * is not supported an exception will be thrown.
     *
     * @param representation the BsonRepresentation.
     * @return a new Codec with the correct representation.
     * @throws CodecConfigurationException if the codec does not support the provided representation
     */
    Codec<T> withRepresentation(BsonType representation);
}
