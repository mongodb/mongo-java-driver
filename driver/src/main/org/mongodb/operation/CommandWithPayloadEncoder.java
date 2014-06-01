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

import org.bson.BSONWriter;
import org.bson.codecs.Encoder;
import org.mongodb.Document;
import org.mongodb.codecs.DocumentCodec;

import java.util.Map;

class CommandWithPayloadEncoder<T> extends DocumentCodec {
    private final Encoder<T> payloadEncoder;
    private final String fieldContainingPayload;

    CommandWithPayloadEncoder(final String fieldContainingPayload, final Encoder<T> payloadEncoder) {
        this.payloadEncoder = payloadEncoder;
        this.fieldContainingPayload = fieldContainingPayload;
    }

    // we need to cast the payload to (T) to encode it
    @SuppressWarnings("unchecked")
    @Override
    public void encode(final BSONWriter writer, final Document value) {
        writer.writeStartDocument();

        for (final Map.Entry<String, Object> entry : value.entrySet()) {
            String fieldName = entry.getKey();

            writer.writeName(fieldName);
            if (fieldContainingPayload.equals(fieldName)) {
                payloadEncoder.encode(writer, (T) entry.getValue());
            } else {
                super.writeValue(writer, entry.getValue());
            }
        }
        writer.writeEndDocument();
    }

    @Override
    public Class<Document> getEncoderClass() {
        return Document.class;
    }
}
