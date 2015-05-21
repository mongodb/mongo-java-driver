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

package com.mongodb.client;

import org.bson.BsonObjectId;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

class NameCodec implements CollectibleCodec<Name> {

    @Override
    public void encode(final BsonWriter writer, final Name n, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        writer.writeString("name", n.getName());
        writer.writeInt32("count", n.getCount());
        writer.writeEndDocument();
    }

    @Override
    public Name decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartDocument();
        String name = reader.readString("_id");
        int count = (int) reader.readDouble("value");

        reader.readEndDocument();
        return new Name(name, count);
    }

    @Override
    public Class<Name> getEncoderClass() {
        return Name.class;
    }

    @Override
    public boolean documentHasId(final Name document) {
        return false;
    }

    @Override
    public BsonObjectId getDocumentId(final Name document) {
        return null;
    }

    @Override
    public Name generateIdIfAbsentFromDocument(final Name document) {
        return document;
    }
}
