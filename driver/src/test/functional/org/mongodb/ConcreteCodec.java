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

package org.mongodb;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.ObjectId;

class ConcreteCodec implements CollectibleCodec<Concrete> {

    @Override
    public void encode(final BSONWriter bsonWriter, final Concrete c) {
        bsonWriter.writeStartDocument();
        if (c.getId() == null) {
            c.setId(new ObjectId());
        }
        bsonWriter.writeObjectId("_id", c.getId());
        bsonWriter.writeString("str", c.getStr());
        bsonWriter.writeInt32("i", c.getI());
        bsonWriter.writeInt64("l", c.getL());
        bsonWriter.writeDouble("d", c.getD());
        bsonWriter.writeDateTime("date", c.getDate());
        bsonWriter.writeEndDocument();
    }

    @Override
    public Concrete decode(final BSONReader reader) {
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
    public Object getId(final Concrete document) {
        return document.getId();
    }
}
