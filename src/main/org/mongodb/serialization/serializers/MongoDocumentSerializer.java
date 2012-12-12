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
 *
 */

package org.mongodb.serialization.serializers;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.mongodb.MongoDocument;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//TODO: handle array type
public class MongoDocumentSerializer implements Serializer<MongoDocument> {
    private final PrimitiveSerializers primitiveSerializers;

    public MongoDocumentSerializer(PrimitiveSerializers primitiveSerializers) {
        this.primitiveSerializers = primitiveSerializers;
    }

    // TODO: deal with options.  C# driver sends different options.  For one, to write _id field first
    @Override
    public void serialize(final BSONWriter bsonWriter, final MongoDocument document, final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            bsonWriter.writeName(entry.getKey());
            writeValue(bsonWriter, entry.getValue(), options);
        }
        bsonWriter.writeEndDocument();
    }

    private void writeValue(final BSONWriter bsonWriter, final Object value, final BsonSerializationOptions options) {
        if (value instanceof MongoDocument) {
            serialize(bsonWriter, (MongoDocument) value, options);
        }
        else if (value instanceof Iterable) {
            serializeArray(bsonWriter, (Iterable) value, options);
        }
        else {
            primitiveSerializers.serialize(bsonWriter, value, options);
        }
    }

    private void serializeArray(final BSONWriter bsonWriter, final Iterable iterable, final BsonSerializationOptions options) {
        bsonWriter.writeStartArray();
        for (Object cur : iterable) {
            writeValue(bsonWriter, cur, options);
        }
        bsonWriter.writeEndArray();
    }

    @Override
    public MongoDocument deserialize(final BSONReader reader, final BsonSerializationOptions options) {
        MongoDocument document = new MongoDocument();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, options, fieldName));
        }

        reader.readEndDocument();

        return document;
    }

    private Object readValue(final BSONReader reader, final BsonSerializationOptions options, final String fieldName) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType.equals(BsonType.DOCUMENT)) {
            return getDocumentDeserializerForField(fieldName).deserialize(reader, options);
        }
        else if (bsonType.equals(BsonType.ARRAY)) {
            return readArray(reader, options);
        }
        else {
            return primitiveSerializers.deserialize(reader, options);
        }
    }

    private List readArray(final BSONReader reader, final BsonSerializationOptions options) {
        reader.readStartArray();
        List list = new ArrayList();  // TODO: figure out a way to change concrete class
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, options, null));   // TODO: why is this a warning?
        }
        reader.readEndArray();
        return list;
    }

    @Override
    public Class<MongoDocument> getSerializationClass() {
        return MongoDocument.class;
    }

    protected PrimitiveSerializers getPrimitiveSerializers() {
        return primitiveSerializers;
    }

    protected Serializer getDocumentDeserializerForField(final String fieldName) {
        return this;
    }

}
