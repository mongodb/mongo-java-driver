/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client;

import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;

import java.util.LinkedHashMap;

import static java.lang.String.format;

public final class ImmutableDocumentCodec implements CollectibleCodec<ImmutableDocument> {
    private final CodecRegistry codecRegistry;
    private static final String ID_FIELD_NAME = "_id";

    public ImmutableDocumentCodec(final CodecRegistry codecRegistry) {
        this.codecRegistry = codecRegistry;
    }

    @Override
    public ImmutableDocument generateIdIfAbsentFromDocument(final ImmutableDocument document) {
        LinkedHashMap<String, Object> mutable = new LinkedHashMap<String, Object>(document);
        mutable.put(ID_FIELD_NAME, new ObjectId());
        return new ImmutableDocument(mutable);
    }

    @Override
    public boolean documentHasId(final ImmutableDocument document) {
        return document.containsKey(ID_FIELD_NAME);
    }

    @Override
    public BsonValue getDocumentId(final ImmutableDocument document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException(format("The document does not contain an %s", ID_FIELD_NAME));
        }
        return document.toBsonDocument(ImmutableDocument.class, codecRegistry).get(ID_FIELD_NAME);
    }

    @Override
    public void encode(final BsonWriter writer, final ImmutableDocument value, final EncoderContext encoderContext) {
        codecRegistry.get(Document.class).encode(writer, new Document(value), encoderContext);
    }

    @Override
    public Class<ImmutableDocument> getEncoderClass() {
        return ImmutableDocument.class;
    }

    @Override
    public ImmutableDocument decode(final BsonReader reader, final DecoderContext decoderContext) {
        Document document = codecRegistry.get(Document.class).decode(reader, decoderContext);
        return new ImmutableDocument(document);
    }
}
