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

package org.bson.codecs;

import org.bson.BSONReader;
import org.bson.BSONType;
import org.bson.BSONWriter;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.configuration.CodecSource;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.bson.types.BsonDocument;
import org.bson.types.BsonElement;
import org.bson.types.BsonValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A codec for BsonDocument instances.
 *
 * @since 3.0
 */
public class BsonDocumentCodec implements Codec<BsonDocument> {
    private static final CodecRegistry DEFAULT_REGISTRY = new RootCodecRegistry(Arrays.<CodecSource>asList(new BsonValueCodecSource()));

    private final CodecRegistry codecRegistry;

    public BsonDocumentCodec() {
        codecRegistry = DEFAULT_REGISTRY;
    }

    public BsonDocumentCodec(final CodecRegistry codecRegistry) {
        if (codecRegistry == null) {
            throw new IllegalArgumentException("Codec registry can not be null");
        }
        this.codecRegistry = codecRegistry;
    }

    /**
     * Gets the {@code CodecRegistry} for this {@code Codec}.
     *
     * @return the registry
     */
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    @Override
    public BsonDocument decode(final BSONReader reader) {
        List<BsonElement> keyValuePairs = new ArrayList<BsonElement>();

        reader.readStartDocument();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            keyValuePairs.add(new BsonElement(fieldName, readValue(reader)));
        }

        reader.readEndDocument();

        return new BsonDocument(keyValuePairs);
    }

    /**
     * This method may be overridden to change the behavior of reading the current value from the given {@code BsonReader}.  It is required
     * that the value be fully consumed before returning.
     *
     * @param reader the read to read the value from
     * @return the non-null value read from the reader
     */
    protected BsonValue readValue(final BSONReader reader) {
        return codecRegistry.get(BsonValueCodecSource.getClassForBsonType(reader.getCurrentBSONType())).decode(reader);
    }

    @Override
    public void encode(final BSONWriter writer, final BsonDocument value) {
        writer.writeStartDocument();

        for (Map.Entry<String, BsonValue> entry : value.entrySet()) {
            writer.writeName(entry.getKey());
            writeValue(writer, entry.getValue());
        }

        writer.writeEndDocument();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeValue(final BSONWriter writer, final BsonValue value) {
        Codec codec = codecRegistry.get(BsonValueCodecSource.getClassForBsonType(value.getBsonType()));
        codec.encode(writer, value);
    }

    @Override
    public Class<BsonDocument> getEncoderClass() {
        return BsonDocument.class;
    }
}
