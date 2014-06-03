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

package org.mongodb.codecs;

import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecSource;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.bson.types.BsonDocument;
import org.bson.types.BsonValue;
import org.mongodb.Document;
import org.mongodb.IdGenerator;

import java.util.Arrays;
import java.util.Map;

import static org.mongodb.assertions.Assertions.notNull;

/**
 * A Codec for Document instances.
 *
 * @see org.mongodb.Document
 * @since 3.0
 */
public class DocumentCodec implements CollectibleCodec<Document> {

    private static final String ID_FIELD_NAME = "_id";
    private static final CodecRegistry DEFAULT_REGISTRY = new RootCodecRegistry(Arrays.<CodecSource>asList(new DocumentCodecSource()));
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
        writeValue(writer, id);
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
    public void encode(final BsonWriter writer, final Document document) {
        writer.writeStartDocument();

        beforeFields(writer, document);

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            if (skipField(entry.getKey())) {
                continue;
            }
            writer.writeName(entry.getKey());
            writeValue(writer, entry.getValue());
        }
        writer.writeEndDocument();
    }

    private void beforeFields(final BsonWriter bsonWriter, final Document document) {
        if (document.containsKey(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final String key) {
        return key.equals(ID_FIELD_NAME);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void writeValue(final BsonWriter writer, final Object value) {
        if (value == null) {
            writer.writeNull();
        } else {
            Codec codec = registry.get(value.getClass());
            codec.encode(writer, value);
        }
    }

    @Override
    public Document decode(final BsonReader reader) {
        Document document = new Document();

        reader.readStartDocument();
        while (reader.readBSONType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, fieldName));
        }

        reader.readEndDocument();

        return document;
    }

    protected Object readValue(final BsonReader reader, final String fieldName) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return null;
        } else {
            return registry.get(bsonTypeClassMap.get(bsonType)).decode(reader);
        }
    }

    @Override
    public Class<Document> getEncoderClass() {
        return Document.class;
    }

}
