/*
 * Copyright 2008-2015 MongoDB, Inc.
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

import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.BsonTypeClassMap;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.IdGenerator;
import org.bson.codecs.ObjectIdGenerator;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.List;

/**
 * A collectible codec for a DBObject.
 *
 * @since 3.0
 */
public class DBObjectCodec extends AbstractDBObjectCodec implements CollectibleCodec<DBObject> {

    private static final String ID_FIELD_NAME = "_id";

    private final IdGenerator idGenerator = new ObjectIdGenerator();

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
     * @param codecRegistry the non-null codec registry
     * @param bsonTypeClassMap the non-null BsonTypeClassMap
     * @param objectFactory the non-null object factory used to create empty DBObject instances when decoding
     */
    public DBObjectCodec(final CodecRegistry codecRegistry, final BsonTypeClassMap bsonTypeClassMap, final DBObjectFactory objectFactory) {
        super(codecRegistry, bsonTypeClassMap, objectFactory);
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

    private void beforeFields(final BsonWriter bsonWriter, final EncoderContext encoderContext, final DBObject document) {
        if (encoderContext.isEncodingCollectibleDocument() && document.containsField(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValue(bsonWriter, null, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final EncoderContext encoderContext, final String key) {
        return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
    }

}

