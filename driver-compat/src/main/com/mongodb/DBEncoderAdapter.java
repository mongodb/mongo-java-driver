/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import org.bson.BSONBinaryReader;
import org.bson.BSONWriter;
import org.bson.ByteBufNIO;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.mongodb.Encoder;
import org.mongodb.IdGenerator;

import static java.nio.ByteBuffer.wrap;

class DBEncoderAdapter implements Encoder<DBObject> {

    private static final String ID_FIELD_NAME = "_id";

    private final DBEncoder encoder;
    private final IdGenerator idGenerator;

    public DBEncoderAdapter(final DBEncoder encoder, final IdGenerator idGenerator) {
        this.encoder = encoder;
        this.idGenerator = idGenerator;
    }

    // TODO: this can be optimized to reduce copying of buffers.  For that we'd need an InputBuffer that could iterate
    //       over an array of ByteBuffer instances from a PooledByteBufferOutputBuffer
    @Override
    public void encode(final BSONWriter bsonWriter, final DBObject document) {
        if (document.get(ID_FIELD_NAME) == null) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }

        BasicOutputBuffer buffer = new BasicOutputBuffer();
        try {
            encoder.writeObject(buffer, document);
            BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(new ByteBufNIO(wrap(buffer.toByteArray()))), true);
            try {
                bsonWriter.pipe(reader);
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
