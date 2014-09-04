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

package com.mongodb.codecs;

import org.bson.codecs.BinaryCodec;
import org.bson.codecs.BsonDBPointerCodec;
import org.bson.codecs.BsonRegularExpressionCodec;
import org.bson.codecs.BsonTimestampCodec;
import org.bson.codecs.BsonUndefinedCodec;
import org.bson.codecs.CodeCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.MaxKeyCodec;
import org.bson.codecs.MinKeyCodec;
import org.bson.codecs.ObjectIdCodec;
import org.bson.codecs.SymbolCodec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.mongodb.CodeWithScope;
import org.mongodb.Document;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@code CodecProvider} for the Document class and all the default Codec implementations on which it depends.
 *
 * @since 3.0
 */
public class DocumentCodecProvider implements CodecProvider {
    private final Map<Class<?>, Codec<?>> codecs = new HashMap<Class<?>, Codec<?>>();
    private final BsonTypeClassMap bsonTypeClassMap;

    /**
     * Construct a new instance with a default {@code BsonTypeClassMap}.
     */
    public DocumentCodecProvider() {
        this(new BsonTypeClassMap());
    }

    /**
     * Construct a new instance with the given instance of {@code BsonTypeClassMap}.
     *
     * @param bsonTypeClassMap the {@code BsonTypeClassMap} with which to construct instances of {@code DocumentCodec} and {@code
     *                         ListCodec}
     */
    public DocumentCodecProvider(final BsonTypeClassMap bsonTypeClassMap) {
        this.bsonTypeClassMap = bsonTypeClassMap;
        addCodecs();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        if (codecs.containsKey(clazz)) {
            return (Codec<T>) codecs.get(clazz);
        }

        if (clazz == CodeWithScope.class) {
            return (Codec<T>) new CodeWithScopeCodec(registry.get(Document.class));
        }

        if (clazz == Document.class) {
            return (Codec<T>) new DocumentCodec(registry, bsonTypeClassMap);
        }

        if (List.class.isAssignableFrom(clazz)) {
            return (Codec<T>) new ListCodec(registry, bsonTypeClassMap);
        }

        return null;
    }

    private void addCodecs() {
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
        addCodec(new SymbolCodec());
        addCodec(new BsonTimestampCodec());
        addCodec(new BsonUndefinedCodec());
    }

    private <T> void addCodec(final Codec<T> codec) {
        codecs.put(codec.getEncoderClass(), codec);
    }
}
