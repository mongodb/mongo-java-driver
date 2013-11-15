/*
 * Copyright (c) 2008 - 2013 MongoDB Inc. <http://10gen.com>
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

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.mongodb.Codec;
import org.mongodb.CollectibleCodec;
import org.mongodb.Decoder;
import org.mongodb.Encoder;

class CompoundDBObjectCodec implements Codec<DBObject>, CollectibleCodec<DBObject> {

    private static final String ID_FIELD_NAME = "_id";

    private final Encoder<? super DBObject> encoder;
    private final Decoder<? extends DBObject> decoder;

    public CompoundDBObjectCodec(final Encoder<? super DBObject> encoder, final Decoder<? extends DBObject> decoder) {
        this.encoder = encoder;
        this.decoder = decoder;
    }

    public CompoundDBObjectCodec(final Codec<DBObject> codec) {
        this(codec, codec);
    }

    @Override
    public DBObject decode(final BSONReader reader) {
        return decoder.decode(reader);
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final DBObject value) {
        encoder.encode(bsonWriter, value);
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
    }

    @Override
    public Object getId(final DBObject document) {
        return document.get(ID_FIELD_NAME);
    }

    public Encoder<? super DBObject> getEncoder() {
        return encoder;
    }

    public Decoder<? extends DBObject> getDecoder() {
        return decoder;
    }
}
