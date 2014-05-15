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

package org.mongodb.codecs;

import org.bson.BSONType;
import org.bson.codecs.BSONTimestampCodec;
import org.bson.codecs.BinaryCodec;
import org.bson.codecs.CodeCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DBPointerCodec;
import org.bson.codecs.MaxKeyCodec;
import org.bson.codecs.MinKeyCodec;
import org.bson.codecs.RegularExpressionCodec;
import org.bson.codecs.SymbolCodec;
import org.bson.codecs.UndefinedCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecSource;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.DBPointer;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.RegularExpression;
import org.bson.types.Symbol;
import org.bson.types.Undefined;
import org.mongodb.Document;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocumentCodecSource implements CodecSource {
    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();
    private final Map<BSONType, Class<?>> bsonTypeClassMap;

    public DocumentCodecSource() {
        this(createDefaultBsonTypeClassMap());
    }

    public DocumentCodecSource(final Map<BSONType, Class<?>> bsonTypeClassMap) {
        this.bsonTypeClassMap = bsonTypeClassMap;
        addCodecs();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (codecs.containsKey(clazz)) {
            return (Codec<T>) codecs.get(clazz);
        }

        if (clazz.equals(CodeWithScope.class)) {
            return (Codec<T>) new CodeWithScopeCodec(registry.get(Document.class));
        }

        if (Document.class.isAssignableFrom(clazz)) {
            return (Codec<T>) new DocumentCodec(registry, bsonTypeClassMap);
        }

        if (List.class.isAssignableFrom(clazz)) {
            return (Codec<T>) new ListCodec(registry, bsonTypeClassMap);
        }

        return null;
    }

    private static Map<BSONType, Class<?>> createDefaultBsonTypeClassMap() {
        Map<BSONType, Class<?>> map = new HashMap<BSONType, Class<?>>();
        map.put(BSONType.ARRAY, List.class);
        map.put(BSONType.BINARY, Binary.class);
        map.put(BSONType.BOOLEAN, Boolean.class);
        map.put(BSONType.DATE_TIME, Date.class);
        map.put(BSONType.DB_POINTER, DBPointer.class);
        map.put(BSONType.DOCUMENT, Document.class);
        map.put(BSONType.DOUBLE, Double.class);
        map.put(BSONType.INT32, Integer.class);
        map.put(BSONType.INT64, Long.class);
        map.put(BSONType.MAX_KEY, MaxKey.class);
        map.put(BSONType.MIN_KEY, MinKey.class);
        map.put(BSONType.JAVASCRIPT, Code.class);
        map.put(BSONType.JAVASCRIPT_WITH_SCOPE, CodeWithScope.class);
        map.put(BSONType.OBJECT_ID, ObjectId.class);
        map.put(BSONType.REGULAR_EXPRESSION, RegularExpression.class);
        map.put(BSONType.STRING, String.class);
        map.put(BSONType.SYMBOL, Symbol.class);
        map.put(BSONType.TIMESTAMP, BSONTimestamp.class);
        map.put(BSONType.UNDEFINED, Undefined.class);

        return map;
    }

    private void addCodecs() {
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
        addCodec(new SymbolCodec());
        addCodec(new BSONTimestampCodec());
        addCodec(new UndefinedCodec());
    }

    private <T> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }
}
