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

import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecSource;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.mongodb.Document;
import org.mongodb.codecs.validators.QueryFieldNameValidator;
import org.mongodb.codecs.validators.Validator;

import java.util.Arrays;
import java.util.Map;

/**
 * A Codec for Document instances.
 *
 * @see org.mongodb.Document
 *
 * @since 3.0
 */
public class DocumentCodec implements Codec<Document> {

    private static final CodecRegistry DEFAULT_REGISTRY = new RootCodecRegistry(Arrays.<CodecSource>asList(new DocumentCodecSource()));
    private static final BsonTypeClassMap DEFAULT_BSON_TYPE_CLASS_MAP = new BsonTypeClassMap();

    private final Validator<String> fieldNameValidator;
    private final BsonTypeClassMap bsonTypeClassMap;
    private final CodecRegistry registry;

    /**
     * Constructs a new instance using the default {@code DocumentCodecSource} and the default BSON type class map.
     * @param fieldNameValidator
     */
    public DocumentCodec(final Validator<String> fieldNameValidator) {
        this.fieldNameValidator = fieldNameValidator;
        this.bsonTypeClassMap = DEFAULT_BSON_TYPE_CLASS_MAP;
        this.registry = DEFAULT_REGISTRY;
    }

    /**
     * Construct a new instance with the given registry and BSON type class map.
     *
     * @param registry the registry
     * @param bsonTypeClassMap the BSON type class map
     */
    public DocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        this.fieldNameValidator = new QueryFieldNameValidator();
        this.registry = registry;
        this.bsonTypeClassMap = bsonTypeClassMap;
    }

    /**
     * Construct a new instance with a default {@code CodecRegistry} and
     */
    public DocumentCodec() {
        this(new QueryFieldNameValidator());
    }

    @Override
    public void encode(final BSONWriter writer, final Document document) {
        writer.writeStartDocument();

        beforeFields(writer, document);

        for (final Map.Entry<String, Object> entry : document.entrySet()) {
            fieldNameValidator.validate(entry.getKey());

            if (skipField(entry.getKey())) {
                continue;
            }
            writer.writeName(entry.getKey());
            writeValue(writer, entry.getValue());
        }
        writer.writeEndDocument();
    }

    protected void beforeFields(final BSONWriter bsonWriter, final Document document) {
    }

    protected boolean skipField(final String key) {
        return false;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void writeValue(final BSONWriter writer, final Object value) {
        if (value == null) {
            writer.writeNull();
        } else {
            Codec codec;
            if (value.getClass() == Document.class) {
                codec = this;
            } else {
                codec = registry.get(value.getClass());
                if (codec == null) {
                    throw new CodecConfigurationException("Could not find codec for class " + value.getClass());
                }
            }
            codec.encode(writer, value);
        }
    }

    @Override
    public Document decode(final BSONReader reader) {
        Document document = new Document();

        reader.readStartDocument();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, fieldName));
        }

        reader.readEndDocument();

        return document;
    }

    protected Object readValue(final BSONReader reader, final String fieldName) {
        BSONType bsonType = reader.getCurrentBSONType();
        if (bsonType == BSONType.NULL) {
            reader.readNull();
            return null;
        } else if (bsonType == BSONType.DOCUMENT) {
            return decode(reader);
        } else {
            return registry.get(bsonTypeClassMap.get(bsonType)).decode(reader);
        }
    }

    @Override
    public Class<Document> getEncoderClass() {
        return Document.class;
    }

}
