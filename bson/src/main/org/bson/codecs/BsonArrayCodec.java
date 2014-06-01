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
import org.bson.types.BsonArray;
import org.bson.types.BsonValue;

import java.util.ArrayList;
import java.util.List;

/**
 * A codec for BsonArray instances.
 *
 * @since 3.0
 */
public class BsonArrayCodec implements Codec<BsonArray> {
    private final CodecRegistry registry;

    public BsonArrayCodec(final CodecRegistry registry) {
        this.registry = registry;
    }

    @Override
    public BsonArray decode(final BSONReader reader) {
        reader.readStartArray();

        List<BsonValue> list = new ArrayList<BsonValue>();
        while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
            list.add(readValue(reader));
        }

        reader.readEndArray();

        return new BsonArray(list);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void encode(final BSONWriter writer, final BsonArray array) {
        writer.writeStartArray();

        for (BsonValue value : array) {
            Codec codec = registry.get(BsonValueCodecSource.getClassForBsonType(value.getBsonType()));
            codec.encode(writer, value);
        }

        writer.writeEndArray();
    }

    @Override
    public Class<BsonArray> getEncoderClass() {
        return BsonArray.class;
    }

    /**
     * This method may be overridden to change the behavior of reading the current value from the given {@code BsonReader}.  It is required
     * that the value be fully consumed before returning.
     *
     * @param reader the read to read the value from
     * @return the non-null value read from the reader
     */
    protected BsonValue readValue(final BSONReader reader) {
        return registry.get(BsonValueCodecSource.getClassForBsonType(reader.getCurrentBSONType())).decode(reader);
    }

}
