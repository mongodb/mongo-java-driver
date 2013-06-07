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

package com.mongodb.codecs;

import com.mongodb.DBEncoder;
import com.mongodb.DBObject;
import org.bson.BSONBinaryReader;
import org.bson.BSONWriter;
import org.bson.io.BasicInputBuffer;
import org.bson.io.BasicOutputBuffer;
import org.mongodb.Encoder;

import java.nio.ByteBuffer;

public class DBEncoderAdapter implements Encoder<DBObject> {
    private final DBEncoder encoder;

    public DBEncoderAdapter(final DBEncoder encoder) {
        this.encoder = encoder;
    }

    // TODO: this can be optimized to reduce copying of buffers.  For that we'd need an InputBuffer that could iterate
    //       over an array of ByteBuffer instances from a PooledByteBufferOutputBuffer
    @Override
    public void encode(final BSONWriter bsonWriter, final DBObject value) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        try {
            encoder.writeObject(buffer, value);
            final BSONBinaryReader reader = new BSONBinaryReader(new BasicInputBuffer(ByteBuffer.wrap(buffer.toByteArray())), true);
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
