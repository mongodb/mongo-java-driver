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
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mongodb.DBRefBase;
import com.mongodb.MongoInternalException;
import com.mongodb.ReflectionDBObject;
import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.BsonType;
import org.bson.types.Binary;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.Serializer;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBObjectSerializer implements Serializer<DBObject> {
    private static final List<String> EMPTY_PATH = Collections.emptyList();

    private final PrimitiveSerializers primitiveSerializers;
    private final DB db;
    private final Map<List<String>, Class<? extends DBObject>> pathToClassMap;
    private final ReflectionDBObject.JavaWrapper wrapper;

    public DBObjectSerializer(final DB db, final PrimitiveSerializers primitiveSerializers,
                              final Class<? extends DBObject> topLevelClass,
                              final HashMap<String, Class<? extends DBObject>> stringPathToClassMap) {
        this.db = db;
        if (primitiveSerializers == null) {
            throw new IllegalArgumentException("primitiveSerializers is null");
        }
        this.primitiveSerializers = primitiveSerializers;
        this.pathToClassMap = createPathToClassMap(topLevelClass, stringPathToClassMap);
        if (ReflectionDBObject.class.isAssignableFrom(topLevelClass)) {
            wrapper = ReflectionDBObject.getWrapper(topLevelClass);
        }
        else {
            wrapper = null;
        }
    }

    @Override
    public void serialize(final BSONWriter bsonWriter, final DBObject document,
                          final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();

        beforeFields(bsonWriter, document, options);

        for (String key : document.keySet()) {
            validateField(key);
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
            validateField(key);
            bsonWriter.writeName(key);
            writeValue(bsonWriter, document.get(key), options);
        }
        bsonWriter.writeEndDocument();
    }

    public void serializeEmbeddedMap(final BSONWriter bsonWriter, final Map<String, Object> document,
                                     final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();

        for (String key : document.keySet()) {
            validateField(key);
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

    protected void validateField(String key) {
    }

    @SuppressWarnings("unchecked")
    protected void writeValue(final BSONWriter bsonWriter, final Object value, final BsonSerializationOptions options) {
        if (value instanceof DBRefBase) {
            serializeDBRef(bsonWriter, (DBRefBase) value, options);
        }
        else if (value instanceof DBObject) {
            serializeEmbeddedDBObject(bsonWriter, (DBObject) value, options);
        }
        else if (value instanceof Map) {
            serializeEmbeddedMap(bsonWriter, (Map<String, Object>) value, options);
        }
        else if (value instanceof Iterable) {
            serializeIterable(bsonWriter, (Iterable) value, options);
        }
        else if (value instanceof byte[]) {
            primitiveSerializers.serialize(bsonWriter, new Binary((byte[]) value), options);
        }
        else if (value != null && value.getClass().isArray()) {
            serializeArray(bsonWriter, value, options);
        }
        else {
            primitiveSerializers.serialize(bsonWriter, value, options);
        }
    }

    private void serializeArray(final BSONWriter bsonWriter, final Object value,
                                final BsonSerializationOptions options) {
        bsonWriter.writeStartArray();

        int size = Array.getLength(value);
        for (int i = 0; i < size; i++) {
            writeValue(bsonWriter, Array.get(value, i), options);
        }

        bsonWriter.writeEndArray();
    }

    private void serializeDBRef(final BSONWriter bsonWriter, final DBRefBase dbRef,
                                final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();

        bsonWriter.writeString("$ref", dbRef.getRef());
        bsonWriter.writeName("$id");
        writeValue(bsonWriter, dbRef.getId(), options);

        bsonWriter.writeEndDocument();
    }

    private void serializeIterable(final BSONWriter bsonWriter, final Iterable iterable,
                                   final BsonSerializationOptions options) {
        bsonWriter.writeStartArray();
        for (final Object cur : iterable) {
            writeValue(bsonWriter, cur, options);
        }
        bsonWriter.writeEndArray();
    }

    @Override
    public DBObject deserialize(final BSONReader reader, final BsonSerializationOptions options) {
        List<String> path = new ArrayList<String>(10);
        final DBObject document = getNewInstance(path);

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, options, fieldName, path));
        }

        reader.readEndDocument();

        return document;
    }

    @Override
    public Class<DBObject> getSerializationClass() {
        return DBObject.class;
    }

    public PrimitiveSerializers getPrimitiveSerializers() {
        return primitiveSerializers;
    }

    public DB getDb() {
        return db;
    }

    public Class<? extends DBObject> getTopLevelClass() {
        return pathToClassMap.get(EMPTY_PATH);
    }

    public Map<List<String>, Class<? extends DBObject>> getPathToClassMap() {
        return pathToClassMap;
    }

    private DBObject getNewInstance(final List<String> path) {
        Class<? extends DBObject> newInstanceClass = null;
        try {
            newInstanceClass = pathToClassMap.get(path);
            if (newInstanceClass == null) {
                if (wrapper != null) {
                    newInstanceClass = wrapper.getInternalClass(path);
                }
                if (newInstanceClass == null) {
                    newInstanceClass = BasicDBObject.class;
                }
            }
            return newInstanceClass.newInstance();
        } catch (InstantiationException e) {
            throw new MongoInternalException("can't create a new instance of class " + newInstanceClass, e);
        } catch (IllegalAccessException e) {
            throw new MongoInternalException("can't create a new instance of class " + newInstanceClass, e);
        }
    }

    private Object deserializeDocument(final BSONReader reader, final BsonSerializationOptions options,
                                       final List<String> path) {
        final DBObject document = getNewInstance(path);

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, options, fieldName, path));
        }

        reader.readEndDocument();

        if (document.containsField("$ref") && document.containsField("$id")) {
            return new DBRef(db, document);
        }


        return document;
    }

    private Object readValue(final BSONReader reader, final BsonSerializationOptions options, final String fieldName,
                             final List<String> path) {
        final BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType.equals(BsonType.DOCUMENT)) {
            path.add(fieldName);
            Object retVal = deserializeDocument(reader, options, path);
            path.remove(path.size() - 1);
            return retVal;
        }
        else if (bsonType.equals(BsonType.ARRAY)) {
            path.add(fieldName);
            Object retVal = readArray(reader, options, path);
            path.remove(path.size() - 1);
            return retVal;
        }
        else {
            return primitiveSerializers.deserialize(reader, options);
        }

    }

    private List readArray(final BSONReader reader, final BsonSerializationOptions options, final List<String> path) {
        reader.readStartArray();
        final BasicDBList list = new BasicDBList();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, options, null, path));   // TODO: why is this a warning?
        }
        reader.readEndArray();
        return list;
    }

    private Map<List<String>, Class<? extends DBObject>> createPathToClassMap(
            final Class<? extends DBObject> topLevelClass,
            final HashMap<String, Class<? extends DBObject>> stringPathToClassMap) {
        Map<List<String>, Class<? extends DBObject>> pathToClassMap = new HashMap<List<String>, Class<? extends DBObject>>();
        pathToClassMap.put(EMPTY_PATH, topLevelClass);
        for (Map.Entry<String, Class<? extends DBObject>> cur : stringPathToClassMap.entrySet()) {
            List<String> path = Arrays.asList(cur.getKey().split("\\."));
            pathToClassMap.put(path, cur.getValue());
        }

        return Collections.unmodifiableMap(pathToClassMap);
    }


}

