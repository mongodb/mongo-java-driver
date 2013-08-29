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

package org.mongodb.operation;

import org.bson.BSONWriter;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.codecs.Codecs;

import java.util.Map;

class CommandWithPayloadEncoder<T> implements Encoder<Document> {
    //Commands should be simple documents with no special types, other than the payload document
    private final Codecs codecs = Codecs.createDefault();

    private final Encoder<T> payloadEncoder;
    private final String fieldContainingPayload;

    CommandWithPayloadEncoder(final String fieldContainingPayload, final Encoder<T> payloadEncoder) {
        this.payloadEncoder = payloadEncoder;
        this.fieldContainingPayload = fieldContainingPayload;
    }

    //we need to cast the payload to (T) to encode it
    @SuppressWarnings("unchecked")
    @Override
    public void encode(final BSONWriter bsonWriter, final Document value) {
        bsonWriter.writeStartDocument();

        for (final Map.Entry<String, Object> entry : value.entrySet()) {
            final String fieldName = entry.getKey();

            bsonWriter.writeName(fieldName);
            if (fieldContainingPayload.equals(fieldName)) {
                payloadEncoder.encode(bsonWriter, (T) entry.getValue());
            } else {
                codecs.encode(bsonWriter, entry.getValue());
            }
        }
        bsonWriter.writeEndDocument();
    }

    @Override
    public Class<Document> getEncoderClass() {
        return Document.class;
    }
}
