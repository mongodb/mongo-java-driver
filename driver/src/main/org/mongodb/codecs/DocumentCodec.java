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

package org.mongodb.codecs;

import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.types.CodeWithScope;
import org.mongodb.Codec;
import org.mongodb.DBRef;
import org.mongodb.Document;
import org.mongodb.codecs.validators.QueryFieldNameValidator;
import org.mongodb.codecs.validators.Validator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: decode into DBRef?
public class DocumentCodec implements Codec<Document> {
    private final PrimitiveCodecs primitiveCodecs;
    private final Validator<String> fieldNameValidator;
    private Codecs codecs;

    public DocumentCodec() {
        this(PrimitiveCodecs.createDefault());
    }

    public DocumentCodec(final PrimitiveCodecs primitiveCodecs) {
        this(primitiveCodecs, new QueryFieldNameValidator());
    }

    protected DocumentCodec(final PrimitiveCodecs primitiveCodecs, final Validator<String> fieldNameValidator) {
        if (primitiveCodecs == null) {
            throw new IllegalArgumentException("primitiveCodecs is null");
        }
        this.fieldNameValidator = fieldNameValidator;
        this.primitiveCodecs = primitiveCodecs;
        codecs = new Codecs(primitiveCodecs, fieldNameValidator);
    }

    @Override
    public void encode(final BSONWriter bsonWriter, final Document document) {
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

    protected void beforeFields(final BSONWriter bsonWriter, final Document document) {
    }

    protected boolean skipField(final String key) {
        return false;
    }

    private void validateFieldName(final String key) {
        fieldNameValidator.validate(key);
    }

    @SuppressWarnings("unchecked")
    protected void writeValue(final BSONWriter bsonWriter, final Object value) {
        // TODO: is this a good idea to allow DBRef to be treated all special?
        // Trish: since it gets decoded as a document, not sure it needs its own encoder
        if (value instanceof DBRef) {
            encodeDBRef(bsonWriter, (DBRef) value);
        } else if (value instanceof CodeWithScope) {
            encodeCodeWithScope(bsonWriter, (CodeWithScope) value);
        } else if (value instanceof Map) {
            encodeMap(bsonWriter, (Map<String, Object>) value);
        } else if (value instanceof Iterable<?>) {
            encodeIterable(bsonWriter, (Iterable) value);
        } else if (value != null && value.getClass().isArray()) {
            encodeArray(bsonWriter, value);
        } else {
            primitiveCodecs.encode(bsonWriter, value);
        }
    }

    private void encodeCodeWithScope(final BSONWriter bsonWriter,
                                     final CodeWithScope codeWithScope) {
        bsonWriter.writeJavaScriptWithScope(codeWithScope.getCode());
        this.encode(bsonWriter, codeWithScope.getScope());
    }

    private void encodeDBRef(final BSONWriter bsonWriter, final DBRef dbRef) {
        codecs.encode(bsonWriter, dbRef);
    }

    private void encodeIterable(final BSONWriter bsonWriter, final Iterable<?> iterable) {
        codecs.encode(bsonWriter, iterable);
    }

    private void encodeArray(final BSONWriter bsonWriter, final Object value) {
        codecs.encode(bsonWriter, value);
    }

    private void encodeMap(final BSONWriter bsonWriter, final Map<String, Object> map) {
        codecs.encode(bsonWriter, map);
    }

    @Override
    public Document decode(final BSONReader reader) {
        final Document document = new Document();

        reader.readStartDocument();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, fieldName));
        }

        reader.readEndDocument();

        return document;
    }

    private Object readValue(final BSONReader reader, final String fieldName) {
        final BSONType bsonType = reader.getCurrentBSONType();
        if (bsonType.equals(BSONType.DOCUMENT)) {
            return getDocumentCodecForField(fieldName).decode(reader);
        } else if (bsonType.equals(BSONType.JAVASCRIPT_WITH_SCOPE)) {
            return readCodeWithScope(reader);
        } else if (bsonType.equals(BSONType.ARRAY)) {
            return readArray(reader);
        } else {
            return primitiveCodecs.decode(reader);
        }
    }

    private CodeWithScope readCodeWithScope(final BSONReader bsonReader) {
        final String code = bsonReader.readJavaScriptWithScope();
        final Document scope = decode(bsonReader);
        return new CodeWithScope(code, scope);
    }

    @SuppressWarnings("unchecked")
    private List<Object> readArray(final BSONReader reader) {
        reader.readStartArray();
        final List<Object> list = new ArrayList<Object>();  // TODO: figure out a way to change concrete class
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            list.add(readValue(reader, null));
        }
        reader.readEndArray();
        return list;
    }

    @Override
    public Class<Document> getEncoderClass() {
        return Document.class;
    }

    protected PrimitiveCodecs getPrimitiveCodecs() {
        return primitiveCodecs;
    }

    protected Codec<Document> getDocumentCodecForField(final String fieldName) {
        return this;
    }
}
