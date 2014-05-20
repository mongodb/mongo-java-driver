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

import org.bson.BSONType;
import org.bson.codecs.BSONTimestampCodec;
import org.bson.codecs.BinaryCodec;
import org.bson.codecs.CodeCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DBPointerCodec;
import org.bson.codecs.MaxKeyCodec;
import org.bson.codecs.MinKeyCodec;
import org.bson.codecs.RegularExpressionCodec;
import org.bson.codecs.UndefinedCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecSource;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.DBPointer;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Undefined;
import org.mongodb.codecs.BooleanCodec;
import org.mongodb.codecs.ByteArrayCodec;
import org.mongodb.codecs.ByteCodec;
import org.mongodb.codecs.DateCodec;
import org.mongodb.codecs.DoubleCodec;
import org.mongodb.codecs.FloatCodec;
import org.mongodb.codecs.IntegerCodec;
import org.mongodb.codecs.LongCodec;
import org.mongodb.codecs.ObjectIdCodec;
import org.mongodb.codecs.PatternCodec;
import org.mongodb.codecs.ShortCodec;
import org.mongodb.codecs.StringCodec;
import org.mongodb.codecs.UUIDCodec;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

class DBObjectCodecSource implements CodecSource {
    private static final Map<BSONType, Class<?>> bsonTypeClassMap = createDefaultBsonTypeClassMap();

    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();

    public static Map<BSONType, Class<?>> getDefaultBsonTypeClassMap() {
        return bsonTypeClassMap;
    }

    public DBObjectCodecSource() {
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

    static Map<BSONType, Class<?>> createDefaultBsonTypeClassMap() {
        Map<BSONType, Class<?>> map = new HashMap<BSONType, Class<?>>();
        map.put(BSONType.BINARY, Binary.class);
        map.put(BSONType.BOOLEAN, Boolean.class);
        map.put(BSONType.DATE_TIME, Date.class);
        map.put(BSONType.DB_POINTER, DBPointer.class);
        map.put(BSONType.DOUBLE, Double.class);
        map.put(BSONType.INT32, Integer.class);
        map.put(BSONType.INT64, Long.class);
        map.put(BSONType.MAX_KEY, MaxKey.class);
        map.put(BSONType.MIN_KEY, MinKey.class);
        map.put(BSONType.JAVASCRIPT, Code.class);
        map.put(BSONType.OBJECT_ID, ObjectId.class);
        map.put(BSONType.REGULAR_EXPRESSION, Pattern.class);
        map.put(BSONType.STRING, String.class);
        map.put(BSONType.SYMBOL, String.class);
        map.put(BSONType.TIMESTAMP, BSONTimestamp.class);
        map.put(BSONType.UNDEFINED, Undefined.class);

        return map;
    }

    private void addCodecs() {
        addCodec(new ByteCodec());
        addCodec(new BinaryCodec());
        addCodec(new BooleanCodec());
        addCodec(new DateCodec());
        addCodec(new DBPointerCodec());
        addCodec(new DoubleCodec());
        addCodec(new IntegerCodec());
        addCodec(new LongCodec());
        addCodec(new MinKeyCodec());
        addCodec(new MaxKeyCodec());
        addCodec(new CodeCodec());
        addCodec(new ObjectIdCodec());
        addCodec(new RegularExpressionCodec());
        addCodec(new StringCodec());
        addCodec(new PatternCodec());
        addCodec(new BSONTimestampCodec());
        addCodec(new UndefinedCodec());
        addCodec(new ShortCodec());
        addCodec(new ByteArrayCodec());
        addCodec(new FloatCodec());
        addCodec(new UUIDCodec());
    }

    private <T> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }
}
