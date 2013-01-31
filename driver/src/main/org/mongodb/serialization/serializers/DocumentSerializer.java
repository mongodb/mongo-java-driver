/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.serialization.serializers;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.bson.types.Binary;
import org.bson.types.Document;
import org.mongodb.DBRef;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: deserialize into DBRef?
public class DocumentSerializer implements Serializer<Document> {
    private final PrimitiveSerializers primitiveSerializers;

    public DocumentSerializer(final PrimitiveSerializers primitiveSerializers) {
        if (primitiveSerializers == null) {
            throw new IllegalArgumentException("primitiveSerializers is null");
        }
        this.primitiveSerializers = primitiveSerializers;
    }

    @Override
    public void serialize(final BSONWriter bsonWriter, final Document document) {
        bsonWriter.writeStartDocument();

        beforeFields(bsonWriter, document);

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            validateFieldName(entry.getKey());

            if (skipField(entry.getKey())) {
                continue;
            }
            bsonWriter.writeName(entry.getKey());
            writeValue(bsonWriter, entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    private void serializeMap(final BSONWriter bsonWriter, final Map<String, Object> document) {
        bsonWriter.writeStartDocument();

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            validateFieldName(entry.getKey());
            bsonWriter.writeName(entry.getKey());
            writeValue(bsonWriter, entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    protected void beforeFields(final BSONWriter bsonWriter, final Document document) {
    }

    protected boolean skipField(final String key) {
        return false;
    }

    protected void validateFieldName(final String key) {
    }

    @SuppressWarnings("unchecked")
    protected void writeValue(final BSONWriter bsonWriter, final Object value) {
        // TODO: is this a good idea to allow DBRef to be treated all special?
        if (value instanceof DBRef) {
            serializeDBRef(bsonWriter, (DBRef) value);
        }
        else if (value instanceof Map) {
            serializeMap(bsonWriter, (Map<String, Object>) value);
        }
        else if (value instanceof Iterable<?>) {
            serializeIterable(bsonWriter, (Iterable) value);
        }
        // TODO: Is this a good idea to allow byte[] to be treated all special?
        else if (value instanceof byte[]) {
            primitiveSerializers.serialize(bsonWriter, new Binary((byte[]) value));
        }
        else if (value != null && value.getClass().isArray()) {
            serializeArray(bsonWriter, value);
        }
        else {
            primitiveSerializers.serialize(bsonWriter, value);
        }
    }

    private void serializeDBRef(final BSONWriter bsonWriter, final DBRef dbRef) {
        bsonWriter.writeStartDocument();

        bsonWriter.writeString("$ref", dbRef.getRef());
        bsonWriter.writeName("$id");
        writeValue(bsonWriter, dbRef.getId());

        bsonWriter.writeEndDocument();
    }

    private void serializeIterable(final BSONWriter bsonWriter, final Iterable<?> iterable) {
        bsonWriter.writeStartArray();
        for (final Object cur : iterable) {
            writeValue(bsonWriter, cur);
        }
        bsonWriter.writeEndArray();
    }

    private void serializeArray(final BSONWriter bsonWriter, final Object value) {
        bsonWriter.writeStartArray();

        final int size = Array.getLength(value);
        for (int i = 0; i < size; i++) {
            writeValue(bsonWriter, Array.get(value, i));
        }

        bsonWriter.writeEndArray();
    }


    @Override
    public Document deserialize(final BSONReader reader) {
        final Document document = new Document();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, fieldName));
        }

        reader.readEndDocument();

        return document;
    }

    private Object readValue(final BSONReader reader, final String fieldName) {
        final BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType.equals(BsonType.DOCUMENT)) {
            return getDocumentDeserializerForField(fieldName).deserialize(reader);
        }
        else if (bsonType.equals(BsonType.ARRAY)) {
            return readArray(reader);
        }
        else {
            return primitiveSerializers.deserialize(reader);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Object> readArray(final BSONReader reader) {
        reader.readStartArray();
        final List<Object> list = new ArrayList<Object>();  // TODO: figure out a way to change concrete class
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, null));
        }
        reader.readEndArray();
        return list;
    }

    @Override
    public Class<Document> getSerializationClass() {
        return Document.class;
    }

    protected PrimitiveSerializers getPrimitiveSerializers() {
        return primitiveSerializers;
    }

    protected Serializer<Document> getDocumentDeserializerForField(final String fieldName) {
        return this;
    }
}
