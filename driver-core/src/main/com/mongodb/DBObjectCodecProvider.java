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

package com.mongodb;

import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.Codec;
import org.bson.codecs.DateCodec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BSONTimestamp;

import java.util.Date;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A provider for a DBObjectCodec.
 *
 * @since 3.0
 */
public class DBObjectCodecProvider implements CodecProvider {
    private final BsonTypeClassMap bsonTypeClassMap;

    /**
     * Construct an instance using the default {@code BsonTypeClassMap}.
     *
     * @see DBObjectCodec#getDefaultBsonTypeClassMap()
     */
    public DBObjectCodecProvider() {
        this(DBObjectCodec.getDefaultBsonTypeClassMap());
    }

    /**
     * Construct an instance with the given {@code BsonTypeClassMap}.
     *
     * @param bsonTypeClassMap the BsonTypeClassMap
     */
    public DBObjectCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
        this.bsonTypeClassMap = notNull("bsonTypeClassMap", bsonTypeClassMap);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (clazz == BSONTimestamp.class) {
            return (Codec<T>) new BSONTimestampCodec();
        }

        if (DBObject.class.isAssignableFrom(clazz) && !List.class.isAssignableFrom(clazz)) {
            return (Codec<T>) new DBObjectCodec(registry, bsonTypeClassMap);
        }

        if (Date.class.isAssignableFrom(clazz)) {
            return (Codec<T>) new DateCodec();
        }

        return null;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}
