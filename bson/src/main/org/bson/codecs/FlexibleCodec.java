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

/**
 * Implementations of this interface can decode additional types
 * and translate them to the desired value type depending on the BsonRepresentation.
 *
 * @param <T> the value type
 * @since 4.2
 */
public interface FlexibleCodec<T> extends Codec<T> {

    /**
     * Sets the BsonRepresentation that should be used by the codec to decide
     * what types of values to decode and how it should translate them.
     *
     * @param bsonRep the BsonRepresentation.
     */
    void setBsonRep(BsonType bsonRep);
}
