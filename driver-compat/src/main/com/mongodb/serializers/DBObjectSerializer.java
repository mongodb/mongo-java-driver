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

package com.mongodb.serializers;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;

import java.util.List;
import java.util.Map;

public class DBObjectSerializer implements Serializer<DBObject> {
    private final PrimitiveSerializers primitiveSerializers;

    public DBObjectSerializer(final PrimitiveSerializers primitiveSerializers) {
        if (primitiveSerializers == null) {
            throw new IllegalArgumentException("primitiveSerializers is null");
        }
        this.primitiveSerializers = primitiveSerializers;
    }

    @Override
    public void serialize(final BSONWriter bsonWriter, final DBObject document,
                          final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();

        beforeFields(bsonWriter, document, options);

        for (String key : document.keySet()) {
            if (skipField(key)) {
                continue;
            }
            bsonWriter.writeName(key);
            writeValue(bsonWriter, document.get(key), options);
        }
        bsonWriter.writeEndDocument();
    }

    public void serializeEmbeddedDBObject(final BSONWriter bsonWriter, final DBObject document,
                                          final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();

        for (String key : document.keySet()) {
            bsonWriter.writeName(key);
            writeValue(bsonWriter, document.get(key), options);
        }
        bsonWriter.writeEndDocument();
    }

    public void serializeEmbeddedMap(final BSONWriter bsonWriter, final Map<String, Object> document,
                                     final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();

        for (String key : document.keySet()) {
            bsonWriter.writeName(key);
            writeValue(bsonWriter, document.get(key), options);
        }
        bsonWriter.writeEndDocument();
    }

    protected void beforeFields(final BSONWriter bsonWriter, final DBObject document,
                                final BsonSerializationOptions options) {
    }

    protected boolean skipField(String key) {
        return false;
    }

    protected void writeValue(final BSONWriter bsonWriter, final Object value, final BsonSerializationOptions options) {
        if (value instanceof DBObject) {
            serializeEmbeddedDBObject(bsonWriter, (DBObject) value, options);
        } else if (value instanceof Map) {
            serializeEmbeddedMap(bsonWriter, (Map<String, Object>) value, options);
        }
        else if (value instanceof Iterable) {
            serializeArray(bsonWriter, (Iterable) value, options);
        }
        else {
            primitiveSerializers.serialize(bsonWriter, value, options);
        }
    }

    private void serializeArray(final BSONWriter bsonWriter, final Iterable iterable,
                                final BsonSerializationOptions options) {
        bsonWriter.writeStartArray();
        for (final Object cur : iterable) {
            writeValue(bsonWriter, cur, options);
        }
        bsonWriter.writeEndArray();
    }

    @Override
    public DBObject deserialize(final BSONReader reader, final BsonSerializationOptions options) {
        final DBObject document = new BasicDBObject();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, options, fieldName));
        }

        reader.readEndDocument();

        return document;
    }

    private Object readValue(final BSONReader reader, final BsonSerializationOptions options, final String fieldName) {
        final BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType.equals(BsonType.DOCUMENT)) {
            return deserialize(reader, options);
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
        final BasicDBList list = new BasicDBList();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, options, null));   // TODO: why is this a warning?
        }
        reader.readEndArray();
        return list;
    }

    @Override
    public Class<DBObject> getSerializationClass() {
        return DBObject.class;
    }
}

