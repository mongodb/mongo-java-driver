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

package com.mongodb.client;

import org.bson.BsonObjectId;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.types.ObjectId;

class ConcreteCodec implements CollectibleCodec<Concrete> {

    @Override
    public void encode(final BsonWriter writer, final Concrete c, final EncoderContext encoderContext) {
        writer.writeStartDocument();
        if (!documentHasId(c)) {
            c.setId(new ObjectId());
        }
        writer.writeObjectId("_id", c.getId());
        writer.writeString("str", c.getStr());
        writer.writeInt32("i", c.getI());
        writer.writeInt64("l", c.getL());
        writer.writeDouble("d", c.getD());
        writer.writeDateTime("date", c.getDate());
        writer.writeEndDocument();
    }

    @Override
    public Concrete decode(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartDocument();
        ObjectId id = reader.readObjectId("_id");
        String str = reader.readString("str");
        int i = reader.readInt32("i");
        long l = reader.readInt64("l");
        double d = reader.readDouble("d");
        long date = reader.readDateTime("date");

        reader.readEndDocument();
        return new Concrete(id, str, i, l, d, date);
    }

    @Override
    public Class<Concrete> getEncoderClass() {
        return Concrete.class;
    }

    @Override
    public boolean documentHasId(final Concrete document) {
        return document.getId() != null;
    }

    @Override
    public BsonObjectId getDocumentId(final Concrete document) {
        return new BsonObjectId(document.getId());
    }

    @Override
    public Concrete generateIdIfAbsentFromDocument(final Concrete document) {
        if (!documentHasId(document)) {
            document.setId(new ObjectId());
        }
        return document;
    }
}
