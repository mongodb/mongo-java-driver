/*
 * Copyright 2008-present MongoDB, Inc.
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

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonReader;
import org.bson.BsonWriter;

/**
 * A Codec
 *
 * @since 3.0
 */
@SuppressWarnings("rawtypes")
public class BsonDocumentWrapperCodec implements Codec<BsonDocumentWrapper> {

    private final Codec<BsonDocument> bsonDocumentCodec;

    /**
     * Construct a new instance,
     *
     * @param bsonDocumentCodec the code to use if the {@code BsonDocumentWrapper} has been unwrapped.
     */
    public BsonDocumentWrapperCodec(final Codec<BsonDocument> bsonDocumentCodec) {
        this.bsonDocumentCodec = bsonDocumentCodec;
    }

    /**
     * Decoding of {@code BsonDocumentWrapper} instances is not supported, so this method will throw {@code UnsupportedOperationException}
     * in all cases.
     *
     * @param reader the BSON reader the reader
     * @param decoderContext a decoder context, currently unused
     * @return the document
     */
    @Override
    public BsonDocumentWrapper decode(final BsonReader reader, final DecoderContext decoderContext) {
        throw new UnsupportedOperationException("Decoding into a BsonDocumentWrapper is not allowed");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void encode(final BsonWriter writer, final BsonDocumentWrapper value, final EncoderContext encoderContext) {
        if (value.isUnwrapped()) {
            bsonDocumentCodec.encode(writer, value, encoderContext);
        } else {
            Encoder encoder = value.getEncoder();
            encoder.encode(writer, value.getWrappedDocument(), encoderContext);
        }
    }

    @Override
    public Class<BsonDocumentWrapper> getEncoderClass() {
        return BsonDocumentWrapper.class;
    }
}
