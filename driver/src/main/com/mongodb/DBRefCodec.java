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
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A Codec for DBRef instances.
 *
 * @since 3.0
 */
public class DBRefCodec implements Codec<DBRef> {
    private final CodecRegistry registry;

    /**
     * Construct an instance with the given registry, which is used to encode the id of the referenced document.
     *
     * @param registry the non-null codec registry
     */
    public DBRefCodec(final CodecRegistry registry) {
        this.registry = notNull("registry", registry);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void encode(final BsonWriter writer, final DBRef value, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeString("$ref", value.getCollectionName());
        writer.writeName("$id");
        Codec codec = registry.get(value.getId().getClass());
        codec.encode(writer, value.getId(), encoderContext);
        if (value.getDatabaseName() != null) {
            writer.writeString("$db", value.getDatabaseName());
        }
        writer.writeEndDocument();
    }

    @Override
    public Class<DBRef> getEncoderClass() {
        return DBRef.class;
    }

    @Override
    public DBRef decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("DBRefCodec does not support decoding");
    }
}
