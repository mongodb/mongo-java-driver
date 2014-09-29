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

package com.mongodb.codecs;

import org.bson.BsonBinarySubType;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.mongodb.Document;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * A Codec for Document instances.
 *
 * @see org.mongodb.Document
 * @since 3.0
 */
public class DocumentCodec implements CollectibleCodec<Document> {

    private static final String ID_FIELD_NAME = "_id";
    private static final CodecRegistry DEFAULT_REGISTRY = new RootCodecRegistry(Arrays.<CodecProvider>asList(new DocumentCodecProvider()));
    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap();

    private final BsonTypeClassMap bsonTypeClassMap;
    private final CodecRegistry registry;
    private final IdGenerator idGenerator;

    /**
     * Construct a new instance with a default {@code CodecRegistry} and
     */
    public DocumentCodec() {
        this(DEFAULT_REGISTRY, DEFAULT_BSON_TYPE_CLASS_MAP, new ObjectIdGenerator());
    }

    /**
     * Construct a new instance with the given registry and BSON type class map.
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     */
    public DocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        this(registry, bsonTypeClassMap, new ObjectIdGenerator());
    }

    /**
     * Construct a new instance with the given registry and BSON type class map.
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     * @param idGenerator the idGenerator to use when generating a value for _id
     */
    public DocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final IdGenerator idGenerator) {
        this.registry = notNull("registry", registry);
        this.bsonTypeClassMap = notNull("bsonTypeClassMap", bsonTypeClassMap);
        this.idGenerator = notNull("idGenerator", idGenerator);
    }

    @Override
    public boolean documentHasId(final Document document) {
        return document.containsKey(ID_FIELD_NAME);
    }

    @Override
    public BsonValue getDocumentId(final Document document) {
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
    public void generateIdIfAbsentFromDocument(final Document document) {
        if (!documentHasId(document)) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
    }

    @Override
    public void encode(final BsonWriter writer, final Document document, final EncoderContext encoderContext) {
        writer.writeStartDocument();

        beforeFields(writer, encoderContext, document);

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            if (skipField(encoderContext, entry.getKey())) {
                continue;
            }
            writer.writeName(entry.getKey());
            writeValue(writer, encoderContext, entry.getValue());
        }
        writer.writeEndDocument();
    }

    @Override
    public Document decode(final BsonReader reader, final DecoderContext decoderContext) {
        Document document = new Document();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, decoderContext));
        }

        reader.readEndDocument();

        return document;
    }

    @Override
    public Class<Document> getEncoderClass() {
        return Document.class;
    }

    private void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext, final Document document) {
        if (encoderContext.isEncodingCollectibleDocument() && document.containsKey(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, encoderContext, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final EncoderContext encoderContext, final String key) {
        return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeValue(final BsonWriter writer, final EncoderContext encoderContext, final Object value) {
        if (value == null) {
            writer.writeNull();
        } else {
            Codec codec = registry.get(value.getClass());
            encoderContext.encodeWithChildContext(codec, writer, value);
        }
    }

    private Object readValue(final BsonReader reader, final DecoderContext decoderContext) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return null;
        } else if (bsonType == BsonType.BINARY) {
            reader.mark();
            byte bsonSubType = reader.readBinaryData().getType();
            reader.reset();
            if (bsonSubType == BsonBinarySubType.UUID_STANDARD.getValue()
                    || bsonSubType == BsonBinarySubType.UUID_LEGACY.getValue()) {
                return registry.get(UUID.class).decode(reader, decoderContext);
            }
        }
        return registry.get(bsonTypeClassMap.get(bsonType)).decode(reader, decoderContext);
    }
}
