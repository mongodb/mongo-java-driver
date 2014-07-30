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

package com.mongodb.operation;

import org.bson.BsonDocumentWrapper;
import org.bson.BsonReader;
import org.bson.BsonValue;
import org.bson.codecs.BsonArrayCodec;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import static org.bson.BsonType.DOCUMENT;

class CommandResultArrayCodec<T> extends BsonArrayCodec {
    private final Decoder<T> payloadDecoder;

    CommandResultArrayCodec(final CodecRegistry registry, final Decoder<T> payloadDecoder) {
        super(registry);
        this.payloadDecoder = payloadDecoder;
    }

    @Override
    protected BsonValue readValue(final BsonReader reader, final DecoderContext decoderContext) {
        if (reader.getCurrentBsonType() == DOCUMENT) {
            return new BsonDocumentWrapper<T>(payloadDecoder.decode(reader, decoderContext), null);
        } else {
            return super.readValue(reader, decoderContext);
        }
    }
}
