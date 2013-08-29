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

import org.bson.BSONReader;
import org.bson.BSONType;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.codecs.Codecs;

import static org.bson.BSONType.DOCUMENT;
import static org.bson.BSONType.END_OF_DOCUMENT;

class CommandResultWithPayloadDecoder<T> implements Decoder<Document> {
    private static final String FIELD_CONTAINING_PAYLOAD = "value";
    private final Decoder<T> payloadDecoder;
    private final Codecs codecs;

    CommandResultWithPayloadDecoder(final Decoder<T> payloadDecoder) {
        this.payloadDecoder = payloadDecoder;
        this.codecs = Codecs.createDefault();
    }

    @Override
    public Document decode(final BSONReader reader) {
        final Document document = new Document();

        reader.readStartDocument();
        while (reader.readBSONType() != END_OF_DOCUMENT) {
            final String fieldName = reader.readName();
            final BSONType bsonType = reader.getCurrentBSONType();
            if (bsonType.equals(DOCUMENT) && fieldName.equals(FIELD_CONTAINING_PAYLOAD)) {
                document.put(fieldName, payloadDecoder.decode(reader));
            } else {
                document.put(fieldName, codecs.decode(reader));
            }
        }
        reader.readEndDocument();
        return document;
    }
}
