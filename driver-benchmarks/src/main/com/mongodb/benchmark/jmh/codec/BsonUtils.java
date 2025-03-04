/*
 * Copyright 2016-present MongoDB, Inc.
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
 *
 */

package com.mongodb.benchmark.jmh.codec;

import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;

public class BsonUtils {
    static final Codec<BsonDocument> BSON_DOCUMENT_CODEC = BsonDocumentCodec();

    public static byte[] getDocumentAsBuffer(final BsonDocument document) throws IOException {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BSON_DOCUMENT_CODEC.encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());

        ByteArrayOutputStream baos = new ByteArrayOutputStream(buffer.getSize());
        buffer.pipe(baos);
        return baos.toByteArray();
    }
}
