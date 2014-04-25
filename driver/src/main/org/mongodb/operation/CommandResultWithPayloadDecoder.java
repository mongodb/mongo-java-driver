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

package org.mongodb.operation;

import org.bson.BSONReader;
import org.bson.BSONType;
import org.mongodb.Decoder;
import org.mongodb.codecs.Codecs;
import org.mongodb.codecs.DocumentCodec;

import java.util.ArrayList;
import java.util.Collection;

import static org.bson.BSONType.ARRAY;
import static org.bson.BSONType.DOCUMENT;

class CommandResultWithPayloadDecoder<T> extends DocumentCodec {
    private final Decoder<T> payloadDecoder;
    private final Codecs codecs;
    private final String fieldContainingPayload;

    CommandResultWithPayloadDecoder(final Decoder<T> payloadDecoder, final String fieldContainingPayload) {
        this.payloadDecoder = payloadDecoder;
        this.fieldContainingPayload = fieldContainingPayload;
        this.codecs = Codecs.createDefault();
    }

    @Override
    protected Object readValue(final BSONReader reader, final String fieldName) {
        BSONType bsonType = reader.getCurrentBSONType();
        if (fieldName.equals(fieldContainingPayload)) {
            if (bsonType.equals(DOCUMENT)) {
                return payloadDecoder.decode(reader);
            } else if (bsonType.equals(ARRAY)) {
                reader.readStartArray();
                Collection<T> collection = new ArrayList<T>();
                while (reader.readBSONType() != BSONType.END_OF_DOCUMENT) {
                    collection.add(payloadDecoder.decode(reader));
                }
                reader.readEndArray();
                return collection;
            } else {
                return codecs.decode(reader);
            }
        } else {
            return super.readValue(reader, fieldName);
        }
    }
}

