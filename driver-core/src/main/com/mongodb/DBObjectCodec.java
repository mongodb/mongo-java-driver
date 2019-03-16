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

package com.mongodb;

import com.mongodb.lang.Nullable;
import org.bson.BSONObject;
import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonDbPointer;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.BsonTypeCodecMap;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.IdGenerator;
import org.bson.codecs.ObjectIdGenerator;
import org.bson.codecs.OverridableUuidRepresentationCodec;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.CodeWScope;
import org.bson.types.Symbol;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Arrays.asList;
import static org.bson.BsonBinarySubType.BINARY;
import static org.bson.BsonBinarySubType.OLD_BINARY;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A collectible codec for a DBObject.
 *
 * @since 3.0
 */
@SuppressWarnings({"rawtypes", "deprecation"})
public class DBObjectCodec implements CollectibleCodec<DBObject>, OverridableUuidRepresentationCodec<DBObject> {
    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = createDefaultBsonTypeClassMap();
    private static final CodecRegistry DEFAULT_REGISTRY =
            fromProviders(asList(new ValueCodecProvider(), new BsonValueCodecProvider(), new DBObjectCodecProvider()));

    private static final String ID_FIELD_NAME = "_id";

    private final CodecRegistry codecRegistry;
    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final DBObjectFactory objectFactory;
    private final IdGenerator idGenerator = new ObjectIdGenerator();
    private final UuidRepresentation uuidRepresentation;

    private static BsonTypeClassMap createDefaultBsonTypeClassMap() {
        Map<BsonType, Class<?>> replacements = new HashMap<BsonType, Class<?>>();
        replacements.put(BsonType.REGULAR_EXPRESSION, Pattern.class);
        replacements.put(BsonType.SYMBOL, String.class);
        replacements.put(BsonType.TIMESTAMP, BSONTimestamp.class);
        replacements.put(BsonType.JAVASCRIPT_WITH_SCOPE, null);
        replacements.put(BsonType.DOCUMENT, null);

        return new BsonTypeClassMap(replacements);
    }

    static BsonTypeClassMap getDefaultBsonTypeClassMap() {
        return DEFAULT_BSON_TYPE_CLASS_MAP;
    }

    static CodecRegistry getDefaultRegistry() {
        return DEFAULT_REGISTRY;
    }

    /**
     * Construct an instance with the default codec registry
     *
     * @since 3.7
     */
    public DBObjectCodec() {
        this(DEFAULT_REGISTRY);
    }

    /**
     * Construct an instance with the given codec registry.
     *
     * @param codecRegistry the non-null codec registry
     */
    public DBObjectCodec(final CodecRegistry codecRegistry) {
        this(codecRegistry, DEFAULT_BSON_TYPE_CLASS_MAP);
    }

    /**
     * Construct an instance.
     *
     * @param codecRegistry the codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     */
    public DBObjectCodec(final CodecRegistry codecRegistry, final BsonTypeClassMap bsonTypeClassMap) {
        this(codecRegistry, bsonTypeClassMap, new BasicDBObjectFactory());
    }

    /**
     * Construct an instance.
     *
     *  @param codecRegistry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     * @param objectFactory the non-null object factory used to create empty DBObject instances when decoding
     */
    public DBObjectCodec(final CodecRegistry codecRegistry, final BsonTypeClassMap bsonTypeClassMap, final DBObjectFactory objectFactory) {
        this(codecRegistry, new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), codecRegistry), objectFactory,
                UuidRepresentation.JAVA_LEGACY);
    }

    private DBObjectCodec(final CodecRegistry codecRegistry, final BsonTypeCodecMap bsonTypeCodecMap, final DBObjectFactory objectFactory,
                         final UuidRepresentation uuidRepresentation) {
        this.objectFactory = notNull("objectFactory", objectFactory);
        this.codecRegistry = notNull("codecRegistry", codecRegistry);
        this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
        this.bsonTypeCodecMap = bsonTypeCodecMap;
    }

    @Override
    public void encode(final BsonWriter writer, final DBObject document, final EncoderContext encoderContext) {
        writer.writeStartDocument();

        beforeFields(writer, encoderContext, document);

        for (final String key : document.keySet()) {
            if (skipField(encoderContext, key)) {
                continue;
            }
            writer.writeName(key);
            writeValue(writer, encoderContext, document.get(key));
        }
        writer.writeEndDocument();
    }

    @Override
    public DBObject decode(final BsonReader reader, final DecoderContext decoderContext) {
        List<String> path = new ArrayList<String>(10);
        return readDocument(reader, decoderContext, path);
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
    }

    @Override
    public boolean documentHasId(final DBObject document) {
        return document.containsField(ID_FIELD_NAME);
    }

    @Override
    public BsonValue getDocumentId(final DBObject document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("The document does not contain an _id");
        }

        Object id = document.get(ID_FIELD_NAME);
        if (id instanceof BsonValue) {
            return (BsonValue) id;
        }

        BsonDocument idHoldingDocument = new BsonDocument();
        BsonWriter writer = new BsonDocumentWriter(idHoldingDocument);
        writer.writeStartDocument();
        writer.writeName(ID_FIELD_NAME);
        writeValue(writer, EncoderContext.builder().build(), id);
        writer.writeEndDocument();
        return idHoldingDocument.get(ID_FIELD_NAME);
    }

    @Override
    public DBObject generateIdIfAbsentFromDocument(final DBObject document) {
        if (!documentHasId(document)) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
        return document;
    }

    @Override
    public Codec<DBObject> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        return new DBObjectCodec(codecRegistry, bsonTypeCodecMap, objectFactory, uuidRepresentation);
    }

    private void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext, final DBObject document) {
        if (encoderContext.isEncodingCollectibleDocument() && document.containsField(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, encoderContext, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final EncoderContext encoderContext, final String key) {
        return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
    }

    @SuppressWarnings("unchecked")
    private void writeValue(final BsonWriter bsonWriter, final EncoderContext encoderContext, @Nullable final Object value) {
        if (value == null) {
            bsonWriter.writeNull();
        } else if (value instanceof DBRef) {
            encodeDBRef(bsonWriter, (DBRef) value, encoderContext);
        } else if (value instanceof Map) {
            encodeMap(bsonWriter, (Map<String, Object>) value, encoderContext);
        } else if (value instanceof Iterable) {
            encodeIterable(bsonWriter, (Iterable) value, encoderContext);
        } else if (value instanceof BSONObject) {
            encodeBsonObject(bsonWriter, (BSONObject) value, encoderContext);
        } else if (value instanceof CodeWScope) {
            encodeCodeWScope(bsonWriter, (CodeWScope) value, encoderContext);
        } else if (value instanceof byte[]) {
            encodeByteArray(bsonWriter, (byte[]) value);
        } else if (value.getClass().isArray()) {
            encodeArray(bsonWriter, value, encoderContext);
        } else if (value instanceof Symbol) {
            bsonWriter.writeSymbol(((Symbol) value).getSymbol());
        } else {
            Codec codec = codecRegistry.get(value.getClass());
            encoderContext.encodeWithChildContext(codec, bsonWriter, value);
        }
    }

    private void encodeMap(final BsonWriter bsonWriter, final Map<String, Object> document, final EncoderContext encoderContext) {
        bsonWriter.writeStartDocument();

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            bsonWriter.writeName(entry.getKey());
            writeValue(bsonWriter, encoderContext.getChildContext(), entry.getValue());
        }
        bsonWriter.writeEndDocument();
    }

    private void encodeBsonObject(final BsonWriter bsonWriter, final BSONObject document, final EncoderContext encoderContext) {
        bsonWriter.writeStartDocument();

        for (String key : document.keySet()) {
            bsonWriter.writeName(key);
            writeValue(bsonWriter, encoderContext.getChildContext(), document.get(key));
        }
        bsonWriter.writeEndDocument();
    }

    private void encodeByteArray(final BsonWriter bsonWriter, final byte[] value) {
        bsonWriter.writeBinaryData(new BsonBinary(value));
    }

    private void encodeArray(final BsonWriter bsonWriter, final Object value, final EncoderContext encoderContext) {
        bsonWriter.writeStartArray();

        int size = Array.getLength(value);
        for (int i = 0; i < size; i++) {
            writeValue(bsonWriter, encoderContext.getChildContext(), Array.get(value, i));
        }

        bsonWriter.writeEndArray();
    }

    private void encodeDBRef(final BsonWriter bsonWriter, final DBRef dbRef, final EncoderContext encoderContext) {
        bsonWriter.writeStartDocument();

        bsonWriter.writeString("$ref", dbRef.getCollectionName());
        bsonWriter.writeName("$id");
        writeValue(bsonWriter, encoderContext.getChildContext(), dbRef.getId());
        if (dbRef.getDatabaseName() != null) {
            bsonWriter.writeString("$db", dbRef.getDatabaseName());
        }
        bsonWriter.writeEndDocument();
    }

    private void encodeCodeWScope(final BsonWriter bsonWriter, final CodeWScope value, final EncoderContext encoderContext) {
        bsonWriter.writeJavaScriptWithScope(value.getCode());
        encodeBsonObject(bsonWriter, value.getScope(), encoderContext.getChildContext());
    }

    private void encodeIterable(final BsonWriter bsonWriter, final Iterable iterable, final EncoderContext encoderContext) {
        bsonWriter.writeStartArray();
        for (final Object cur : iterable) {
            writeValue(bsonWriter, encoderContext.getChildContext(), cur);
        }
        bsonWriter.writeEndArray();
    }

    @Nullable private Object readValue(final BsonReader reader, final DecoderContext decoderContext, @Nullable final String fieldName,
                             final List<String> path) {
        Object initialRetVal;
        BsonType bsonType = reader.getCurrentBsonType();

        if (bsonType.isContainer() && fieldName != null) {
            //if we got into some new context like nested document or array
            path.add(fieldName);
        }

        switch (bsonType) {
            case DOCUMENT:
                initialRetVal = verifyForDBRef(readDocument(reader, decoderContext, path));
                break;
            case ARRAY:
                initialRetVal = readArray(reader, decoderContext, path);
                break;
            case JAVASCRIPT_WITH_SCOPE: //custom for driver-compat types
                initialRetVal = readCodeWScope(reader, decoderContext, path);
                break;
            case DB_POINTER: //custom for driver-compat types
                BsonDbPointer dbPointer = reader.readDBPointer();
                initialRetVal = new DBRef(dbPointer.getNamespace(), dbPointer.getId());
                break;
            case BINARY:
                initialRetVal = readBinary(reader, decoderContext);
                break;
            case NULL:
                reader.readNull();
                initialRetVal = null;
                break;
            default:
                initialRetVal = bsonTypeCodecMap.get(bsonType).decode(reader, decoderContext);
        }

        if (bsonType.isContainer() && fieldName != null) {
            //step out of current context to a parent
            path.remove(fieldName);
        }

        return initialRetVal;
    }

    private Object readBinary(final BsonReader reader, final DecoderContext decoderContext) {
        byte bsonBinarySubType = reader.peekBinarySubType();
        Codec<?> codec;

        if (BsonBinarySubType.isUuid(bsonBinarySubType) && reader.peekBinarySize() == 16) {
            codec = codecRegistry.get(Binary.class);
            switch (bsonBinarySubType) {
                case 3:
                    if (uuidRepresentation == UuidRepresentation.JAVA_LEGACY
                            || uuidRepresentation == UuidRepresentation.C_SHARP_LEGACY
                            || uuidRepresentation == UuidRepresentation.PYTHON_LEGACY) {
                        codec = codecRegistry.get(UUID.class);
                    }
                    break;
                case 4:
                    if (uuidRepresentation == UuidRepresentation.JAVA_LEGACY || uuidRepresentation == UuidRepresentation.STANDARD) {
                        codec = codecRegistry.get(UUID.class);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unknown UUID binary subtype " + bsonBinarySubType);
            }
        } else if (bsonBinarySubType == BINARY.getValue() || bsonBinarySubType == OLD_BINARY.getValue()) {
            codec = codecRegistry.get(byte[].class);
        } else {
            codec =  codecRegistry.get(Binary.class);
        }
        return codec.decode(reader, decoderContext);
    }

    private List readArray(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        reader.readStartArray();
        BasicDBList list = new BasicDBList();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext, null, path));
        }
        reader.readEndArray();
        return list;
    }

    private DBObject readDocument(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        DBObject document = objectFactory.getInstance(path);

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, decoderContext, fieldName, path));
        }

        reader.readEndDocument();
        return document;
    }

    private CodeWScope readCodeWScope(final BsonReader reader, final DecoderContext decoderContext, final List<String> path) {
        return new CodeWScope(reader.readJavaScriptWithScope(), readDocument(reader, decoderContext, path));
    }

    private Object verifyForDBRef(final DBObject document) {
        if (document.containsField("$ref") && document.containsField("$id")) {
            return new DBRef((String) document.get("$db"), (String) document.get("$ref"), document.get("$id"));
        } else {
            return document;
        }
    }
}

