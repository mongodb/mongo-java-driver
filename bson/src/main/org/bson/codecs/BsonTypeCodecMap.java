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
import org.bson.codecs.configuration.CodecRegistry;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.notNull;

/**
 * An efficient map of BsonType to Codec
 *
 * @since 3.3
 */
public class BsonTypeCodecMap {
    private final BsonTypeClassMap bsonTypeClassMap;
    private final Codec<?>[] codecs = new Codec<?>[256];

    /**
     * Initializes the map by iterating the keys of the given BsonTypeClassMap and looking up the Codec for the Class mapped to each key.
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     * @param codecRegistry the non-null CodecRegistry
     */
    public BsonTypeCodecMap(final BsonTypeClassMap bsonTypeClassMap, final CodecRegistry codecRegistry) {
        this.bsonTypeClassMap = notNull("bsonTypeClassMap", bsonTypeClassMap);
        notNull("codecRegistry", codecRegistry);
        for (BsonType cur : bsonTypeClassMap.keys()) {
            Class<?> clazz = bsonTypeClassMap.get(cur);
            if (clazz != null) {
                try {
                    codecs[cur.getValue()] = codecRegistry.get(clazz);
                } catch (CodecConfigurationException e) {
                    // delay reporting this until the codec is actually requested
                }
            }
        }
    }

    /**
     * Gets the Codec mapped to the given bson type.
     *
     * @param bsonType the non-null BsonType
     * @return the non-null Codec
     */
    public Codec<?> get(final BsonType bsonType) {
        Codec<?> codec = codecs[bsonType.getValue()];
        if (codec == null) {
            Class<?> clazz = bsonTypeClassMap.get(bsonType);
            if (clazz == null) {
                throw new CodecConfigurationException(format("No class mapped for BSON type %s.", bsonType));
            } else {
                throw new CodecConfigurationException(format("Can't find a codec for %s.", clazz));
            }
        }
        return codec;
    }
}
