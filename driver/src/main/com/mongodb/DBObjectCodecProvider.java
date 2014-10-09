/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.BsonDbPointer;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.codecs.BinaryCodec;
import org.bson.codecs.BooleanCodec;
import org.bson.codecs.BsonDBPointerCodec;
import org.bson.codecs.BsonRegularExpressionCodec;
import org.bson.codecs.BsonUndefinedCodec;
import org.bson.codecs.ByteArrayCodec;
import org.bson.codecs.ByteCodec;
import org.bson.codecs.CodeCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DateCodec;
import org.bson.codecs.DoubleCodec;
import org.bson.codecs.FloatCodec;
import org.bson.codecs.IntegerCodec;
import org.bson.codecs.LongCodec;
import org.bson.codecs.MaxKeyCodec;
import org.bson.codecs.MinKeyCodec;
import org.bson.codecs.ObjectIdCodec;
import org.bson.codecs.PatternCodec;
import org.bson.codecs.ShortCodec;
import org.bson.codecs.StringCodec;
import org.bson.codecs.UuidCodec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

class DBObjectCodecProvider implements CodecProvider {
    private static final Map<BsonType, Class<?>> bsonTypeClassMap = createDefaultBsonTypeClassMap();

    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();

    public static Map<BsonType, Class<?>> getDefaultBsonTypeClassMap() {
        return bsonTypeClassMap;
    }

    public DBObjectCodecProvider() {
        addCodecs();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (codecs.containsKey(clazz)) {
            return (Codec<T>) codecs.get(clazz);
        }

        return null;
    }

    static Map<BsonType, Class<?>> createDefaultBsonTypeClassMap() {
        Map<BsonType, Class<?>> map = new HashMap<BsonType, Class<?>>();
        map.put(BsonType.BINARY, Binary.class);
        map.put(BsonType.BOOLEAN, Boolean.class);
        map.put(BsonType.DATE_TIME, Date.class);
        map.put(BsonType.DB_POINTER, BsonDbPointer.class);
        map.put(BsonType.DOUBLE, Double.class);
        map.put(BsonType.INT32, Integer.class);
        map.put(BsonType.INT64, Long.class);
        map.put(BsonType.MAX_KEY, MaxKey.class);
        map.put(BsonType.MIN_KEY, MinKey.class);
        map.put(BsonType.JAVASCRIPT, Code.class);
        map.put(BsonType.OBJECT_ID, ObjectId.class);
        map.put(BsonType.REGULAR_EXPRESSION, Pattern.class);
        map.put(BsonType.STRING, String.class);
        map.put(BsonType.SYMBOL, String.class);
        map.put(BsonType.TIMESTAMP, BSONTimestamp.class);
        map.put(BsonType.UNDEFINED, BsonUndefined.class);

        return map;
    }

    private void addCodecs() {
        addCodec(new ByteCodec());
        addCodec(new BinaryCodec());
        addCodec(new BooleanCodec());
        addCodec(new DateCodec());
        addCodec(new BsonDBPointerCodec());
        addCodec(new DoubleCodec());
        addCodec(new IntegerCodec());
        addCodec(new LongCodec());
        addCodec(new MinKeyCodec());
        addCodec(new MaxKeyCodec());
        addCodec(new CodeCodec());
        addCodec(new ObjectIdCodec());
        addCodec(new BsonRegularExpressionCodec());
        addCodec(new StringCodec());
        addCodec(new PatternCodec());
        addCodec(new BSONTimestampCodec());
        addCodec(new BsonUndefinedCodec());
        addCodec(new ShortCodec());
        addCodec(new ByteArrayCodec());
        addCodec(new FloatCodec());
        addCodec(new UuidCodec());
    }

    private <T> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
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
