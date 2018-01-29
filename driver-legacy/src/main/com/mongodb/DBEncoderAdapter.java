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

package com.mongodb;

import org.bson.BsonBinaryReader;
import org.bson.BsonWriter;
import org.bson.ByteBufNIO;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;

import static com.mongodb.assertions.Assertions.notNull;
import static java.nio.ByteBuffer.wrap;

class DBEncoderAdapter implements Encoder<DBObject> {

    private final DBEncoder encoder;

    DBEncoderAdapter(final DBEncoder encoder) {
        this.encoder = notNull("encoder", encoder);
    }

    // TODO: this can be optimized to reduce copying of buffers.  For that we'd need an InputBuffer that could iterate
    //       over an array of ByteBuffer instances from a PooledByteBufferOutputBuffer
    @Override
    public void encode(final BsonWriter writer, final DBObject document, final EncoderContext encoderContext) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        try {
            encoder.writeObject(buffer, document);
            BsonBinaryReader reader = new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(wrap(buffer.toByteArray()))));
            try {
                writer.pipe(reader);
            } finally {
                reader.close();
            }
        } finally {
            buffer.close();
        }
    }

    @Override
    public Class<DBObject> getEncoderClass() {
        return DBObject.class;
    }
}
