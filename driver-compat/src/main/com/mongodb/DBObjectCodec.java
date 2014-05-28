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

import org.bson.BSON;
import org.bson.BSONBinarySubType;
import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.CodeWScope;
import org.bson.types.DBPointer;
import org.bson.types.Symbol;
import org.mongodb.IdGenerator;
import org.mongodb.MongoException;
import org.mongodb.codecs.BinaryToByteArrayTransformer;
import org.mongodb.codecs.BinaryToUUIDTransformer;
import org.mongodb.codecs.CollectibleCodec;
import org.mongodb.codecs.validators.Validator;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.mongodb.MongoExceptions.mapException;

@SuppressWarnings("rawtypes")
class DBObjectCodec implements CollectibleCodec<DBObject> {
    private static final String ID_FIELD_NAME = "_id";

    private final CodecRegistry codecRegistry;
    private final Map<BSONType, Class<?>> bsonTypeClassMap;
    private final Validator<String> fieldNameValidator;
    private final DB db;
    private final DBObjectFactory objectFactory;
    private final IdGenerator idGenerator;

    public DBObjectCodec(final CodecRegistry codecRegistry, final Map<BSONType, Class<?>> bsonTypeClassMap) {
        this.codecRegistry = codecRegistry;
        this.bsonTypeClassMap = bsonTypeClassMap;
        this.idGenerator = null;
        this.db = null;
        this.fieldNameValidator = null;
        this.objectFactory = null;
    }

    public DBObjectCodec(final DB db, final Validator<String> fieldNameValidator, final DBObjectFactory objectFactory,
                         final CodecRegistry codecRegistry, final Map<BSONType, Class<?>> bsonTypeClassMap,
                         final IdGenerator idGenerator) {
        this.db = db;
        this.fieldNameValidator = fieldNameValidator;
        this.objectFactory = objectFactory;
        this.codecRegistry = codecRegistry;
        this.bsonTypeClassMap = bsonTypeClassMap;
        this.idGenerator = idGenerator;
    }

    //TODO: what about BSON Exceptions?
    @Override
    public void encode(final BSONWriter writer, final DBObject document) {
        writer.writeStartDocument();

        beforeFields(writer, document);

        for (final String key : document.keySet()) {
            validateField(key);
            if (skipField(key)) {
                continue;
            }
            writer.writeName(key);
            writeValue(writer, document.get(key));
        }
        writer.writeEndDocument();
    }

    @Override
    public DBObject decode(final BSONReader reader) {
        List<String> path = new ArrayList<String>(10);
        return readDocument(reader, path);
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
    }

    @Override
    public boolean documentHasId(final DBObject document) {
        return document.containsKey(ID_FIELD_NAME);
    }

    @Override
    public Object getDocumentId(final DBObject document) {
        if (documentHasId(document)) {
            return document.get(ID_FIELD_NAME);
//            BsonDocument idHoldingDocument = new BsonDocument();
//            BSONWriter writer = new BsonDocumentWriter(idHoldingDocument);
//            writer.writeStartDocument();
//            writer.writeName(ID_FIELD_NAME);
//            writeValue(writer, document.get(ID_FIELD_NAME));
//            writer.writeEndDocument();
//            return idHoldingDocument.get(ID_FIELD_NAME);
        } else {
            return null;
        }
    }

    @Override
    public void generateIdIfAbsentFromDocument(final DBObject document) {
        if (!documentHasId(document)) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
    }

    private void beforeFields(final BSONWriter bsonWriter, final DBObject document) {
        if (document.containsField(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final String key) {
        return key.equals(ID_FIELD_NAME);
    }

    private void validateField(final String key) {
        fieldNameValidator.validate(key);
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final BSONWriter bsonWriter, final Object initialValue) {
        Object value = BSON.applyEncodingHooks(initialValue);
        try {
            if (value == null) {
                bsonWriter.writeNull();
            } else if (value instanceof DBRefBase) {
                encodeDBRef(bsonWriter, (DBRefBase) value);
            } else if (value instanceof BasicBSONList) {
                encodeIterable(bsonWriter, (BasicBSONList) value);
            } else if (value instanceof DBObject) {
                encodeEmbeddedObject(bsonWriter, ((DBObject) value).toMap());
            } else if (value instanceof Map) {
                encodeEmbeddedObject(bsonWriter, (Map<String, Object>) value);
            } else if (value instanceof Iterable) {
                encodeIterable(bsonWriter, (Iterable) value);
            } else if (value instanceof CodeWScope) {
                encodeCodeWScope(bsonWriter, (CodeWScope) value);
            } else if (value instanceof byte[]) {
                encodeByteArray(bsonWriter, (byte[]) value);
            } else if (value.getClass().isArray()) {
                encodeArray(bsonWriter, value);
            } else if (value instanceof Symbol) {
                bsonWriter.writeSymbol(((Symbol) value).getSymbol());
            } else {
                Codec codec = codecRegistry.get(value.getClass());
                codec.encode(bsonWriter, value);
            }
        } catch (final MongoException e) {
            throw mapException(e);
        }
    }

    private void encodeEmbeddedObject(final BSONWriter bsonWriter, final Map<String, Object> document) {
        bsonWriter.writeStartDocument();

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            validateField(entry.getKey());
            bsonWriter.writeName(entry.getKey());
            writeValue(bsonWriter, entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    private void encodeByteArray(final BSONWriter bsonWriter, final byte[] value) {
        bsonWriter.writeBinaryData(new Binary(value));
    }

    private void encodeArray(final BSONWriter bsonWriter, final Object value) {
        bsonWriter.writeStartArray();

        int size = Array.getLength(value);
        for (int i = 0; i < size; i++) {
            writeValue(bsonWriter, Array.get(value, i));
        }

        bsonWriter.writeEndArray();
    }

    private void encodeDBRef(final BSONWriter bsonWriter, final DBRefBase dbRef) {
        bsonWriter.writeStartDocument();

        bsonWriter.writeString("$ref", dbRef.getRef());
        bsonWriter.writeName("$id");
        writeValue(bsonWriter, dbRef.getId());

        bsonWriter.writeEndDocument();
    }

    @SuppressWarnings("unchecked")
    private void encodeCodeWScope(final BSONWriter bsonWriter, final CodeWScope value) {
        bsonWriter.writeJavaScriptWithScope(value.getCode());
        encodeEmbeddedObject(bsonWriter, value.getScope().toMap());
    }

    private void encodeIterable(final BSONWriter bsonWriter, final Iterable iterable) {
        bsonWriter.writeStartArray();
        for (final Object cur : iterable) {
            writeValue(bsonWriter, cur);
        }
        bsonWriter.writeEndArray();
    }

    private Object readValue(final BSONReader reader, final String fieldName, final List<String> path) {
        Object initialRetVal;
        try {
            BSONType bsonType = reader.getCurrentBSONType();

            if (bsonType.isContainer() && fieldName != null) {
                //if we got into some new context like nested document or array
                path.add(fieldName);
            }

            switch (bsonType) {
                case DOCUMENT:
                    initialRetVal = verifyForDBRef(readDocument(reader, path));
                    break;
                case ARRAY:
                    initialRetVal = readArray(reader, path);
                    break;
                case JAVASCRIPT_WITH_SCOPE: //custom for driver-compat types
                    initialRetVal = readCodeWScope(reader, path);
                    break;
                case DB_POINTER: //custom for driver-compat types
                    DBPointer dbPointer = reader.readDBPointer();
                    initialRetVal = new DBRef(db, dbPointer.getNamespace(), dbPointer.getId());
                    break;
                case BINARY:
                    initialRetVal = readBinary(reader);
                    break;
                case NULL:
                    reader.readNull();
                    initialRetVal = null;
                    break;
                default:
                    initialRetVal = codecRegistry.get(bsonTypeClassMap.get(bsonType)).decode(reader);
            }

            if (bsonType.isContainer() && fieldName != null) {
                //step out of current context to a parent
                path.remove(fieldName);
            }
        } catch (MongoException e) {
            throw mapException(e);
        }

        return BSON.applyDecodingHooks(initialRetVal);
    }

    private Object readBinary(final BSONReader reader) {
        Binary binary = reader.readBinaryData();
        if (binary.getType() == BSONBinarySubType.BINARY.getValue()) {
            return new BinaryToByteArrayTransformer().transform(binary);
        } else if (binary.getType() == BSONBinarySubType.OLD_BINARY.getValue()) {
            return new BinaryToByteArrayTransformer().transform(binary);
        } else if (binary.getType() == BSONBinarySubType.UUID_LEGACY.getValue()) {
            return new BinaryToUUIDTransformer().transform(binary);
        } else {
            return binary;
        }
    }

    private List readArray(final BSONReader reader, final List<String> path) {
        reader.readStartArray();
        BasicDBList list = new BasicDBList();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            list.add(readValue(reader, null, path));   // TODO: why is this a warning?
        }
        reader.readEndArray();
        return list;
    }

    private DBObject readDocument(final BSONReader reader, final List<String> path) {
        DBObject document = objectFactory.getInstance(path);

        reader.readStartDocument();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, fieldName, path));
        }

        reader.readEndDocument();
        return document;
    }

    private CodeWScope readCodeWScope(final BSONReader reader, final List<String> path) {
        return new CodeWScope(reader.readJavaScriptWithScope(), readDocument(reader, path));
    }

    private Object verifyForDBRef(final DBObject document) {
        if (document.containsField("$ref") && document.containsField("$id")) {
            return new DBRef(db, document);
        } else {
            return document;
        }
    }
}

