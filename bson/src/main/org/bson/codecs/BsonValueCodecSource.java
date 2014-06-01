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

package org.bson.codecs;

import org.bson.BSONType;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecSource;
import org.bson.types.Binary;
import org.bson.types.BsonArray;
import org.bson.types.BsonBoolean;
import org.bson.types.BsonDateTime;
import org.bson.types.BsonDocument;
import org.bson.types.BsonDocumentWrapper;
import org.bson.types.BsonDouble;
import org.bson.types.BsonInt32;
import org.bson.types.BsonInt64;
import org.bson.types.BsonNull;
import org.bson.types.BsonString;
import org.bson.types.BsonValue;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.DBPointer;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.types.Symbol;
import org.bson.types.Timestamp;
import org.bson.types.Undefined;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A CodecSource for all subclass of BsonValue.
 *
 * @since 3.0
 */
public class BsonValueCodecSource implements CodecSource {
    private static final Map<BSONType, Class<? extends BsonValue>> DEFAULT_BSON_TYPE_CLASS_MAP;

    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();

    /**
     * Construct a new instance with the default codec for each BSON type.
     */
    public BsonValueCodecSource() {
        addCodecs();
    }

    public static Class<? extends BsonValue> getClassForBsonType(final BSONType bsonType) {
        return DEFAULT_BSON_TYPE_CLASS_MAP.get(bsonType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (codecs.containsKey(clazz)) {
            return (Codec<T>) codecs.get(clazz);
        }

        if (clazz == BsonArray.class) {
            return (Codec<T>) new BsonArrayCodec(registry);
        }

        if (clazz == BsonDocument.class) {
            return (Codec<T>) new BsonDocumentCodec(registry);
        }

        if (clazz == BsonDocumentWrapper.class) {
            return (Codec<T>) new BsonDocumentWrapperCodec(registry.get(BsonDocument.class));
        }

        if (clazz == CodeWithScope.class) {
            return (Codec<T>) new CodeWithScopeCodec(registry.get(BsonDocument.class));
        }

        return null;
    }

    private void addCodecs() {
        addCodec(new BsonNullCodec());
        addCodec(new BinaryCodec());
        addCodec(new BsonBooleanCodec());
        addCodec(new BsonDateTimeCodec());
        addCodec(new DBPointerCodec());
        addCodec(new BsonDoubleCodec());
        addCodec(new BsonInt32Codec());
        addCodec(new BsonInt64Codec());
        addCodec(new MinKeyCodec());
        addCodec(new MaxKeyCodec());
        addCodec(new CodeCodec());
        addCodec(new ObjectIdCodec());
        addCodec(new RegularExpressionCodec());
        addCodec(new BsonStringCodec());
        addCodec(new SymbolCodec());
        addCodec(new TimestampCodec());
        addCodec(new UndefinedCodec());
    }

    private <T> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }

    static {
        Map<BSONType, Class<? extends BsonValue>> map = new HashMap<BSONType, Class<? extends BsonValue>>();

        map.put(BSONType.NULL, BsonNull.class);
        map.put(BSONType.ARRAY, BsonArray.class);
        map.put(BSONType.BINARY, Binary.class);
        map.put(BSONType.BOOLEAN, BsonBoolean.class);
        map.put(BSONType.DATE_TIME, BsonDateTime.class);
        map.put(BSONType.DB_POINTER, DBPointer.class);
        map.put(BSONType.DOCUMENT, BsonDocument.class);
        map.put(BSONType.DOUBLE, BsonDouble.class);
        map.put(BSONType.INT32, BsonInt32.class);
        map.put(BSONType.INT64, BsonInt64.class);
        map.put(BSONType.MAX_KEY, MaxKey.class);
        map.put(BSONType.MIN_KEY, MinKey.class);
        map.put(BSONType.JAVASCRIPT, Code.class);
        map.put(BSONType.JAVASCRIPT_WITH_SCOPE, CodeWithScope.class);
        map.put(BSONType.OBJECT_ID, ObjectId.class);
        map.put(BSONType.REGULAR_EXPRESSION, RegularExpression.class);
        map.put(BSONType.STRING, BsonString.class);
        map.put(BSONType.SYMBOL, Symbol.class);
        map.put(BSONType.TIMESTAMP, Timestamp.class);
        map.put(BSONType.UNDEFINED, Undefined.class);

        DEFAULT_BSON_TYPE_CLASS_MAP = Collections.unmodifiableMap(map);
    }
}
