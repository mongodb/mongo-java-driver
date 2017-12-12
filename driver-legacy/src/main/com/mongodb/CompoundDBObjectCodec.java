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

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;

class CompoundDBObjectCodec implements Codec<DBObject> {

    private final Encoder<DBObject> encoder;
    private final Decoder<DBObject> decoder;

    CompoundDBObjectCodec(final Encoder<DBObject> encoder, final Decoder<DBObject> decoder) {
        this.encoder = encoder;
        this.decoder = decoder;
    }

    CompoundDBObjectCodec(final Codec<DBObject> codec) {
        this(codec, codec);
    }

    @Override
    public DBObject decode(final BsonReader reader, final DecoderContext decoderContext) {
        return decoder.decode(reader, decoderContext);
    }

    @Override
    public void encode(final BsonWriter writer, final DBObject value, final EncoderContext encoderContext) {
        encoder.encode(writer, value, encoderContext);
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
    }

    public Encoder<DBObject> getEncoder() {
        return encoder;
    }

    public Decoder<DBObject> getDecoder() {
        return decoder;
    }
}
