/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
import org.bson.BsonType;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;

// TODO: Add support for generating _id if missing and putting it first
// TODO: Add support for lists
public class DBObjectSerializer implements Serializer<DBObject> {
    private final PrimitiveSerializers primitiveSerializers;

    public DBObjectSerializer(final PrimitiveSerializers primitiveSerializers) {
        this.primitiveSerializers = primitiveSerializers;
    }

    // TODO: deal with options.  C# driver sends different options.  For one, to write _id field first
    @Override
    public void serialize(final BSONWriter bsonWriter, final DBObject document, final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();
        for (final String field : document.keySet()) {
            bsonWriter.writeName(field);
            final Object value = document.get(field);
            if (value instanceof DBObject) {
                serialize(bsonWriter, (DBObject) value, options);
            }
            else {
                primitiveSerializers.serialize(bsonWriter, value, options);
            }
        }
        bsonWriter.writeEndDocument();
    }

    @Override
    public DBObject deserialize(final BSONReader reader, final BsonSerializationOptions options) {
        final DBObject document = new BasicDBObject();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            final BsonType bsonType = reader.getNextBsonType();
            if (bsonType.equals(BsonType.DOCUMENT)) {
                deserialize(reader, options);
            }
            else {
                final Object value = primitiveSerializers.deserialize(reader, options);
                document.put(fieldName, value);
            }
        }

        reader.readEndDocument();

        return document;
    }

    @Override
    public Class<DBObject> getSerializationClass() {
        return DBObject.class;
    }

}
