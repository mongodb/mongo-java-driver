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

package com.mongodb;

import com.mongodb.codecs.DBObjectCodec;
import org.bson.BSONBinaryWriter;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.io.OutputBuffer;

public class DefaultDBEncoder extends BasicBSONEncoder implements DBEncoder {

    @Override
    public int writeObject(final OutputBuffer outputBuffer, final BSONObject document) {
        set(outputBuffer);
        int x = super.putObject(document);
        done();
        return x;
    }

    @Override
    public int putObject(final BSONObject document) {
        final OutputBuffer buffer = getOutputBuffer();
        int startPosition = buffer.getPosition();
        final BSONBinaryWriter writer = new BSONBinaryWriter(buffer, false);
        try {
            new DBObjectCodec().encode(writer, (DBObject) document); //TODO: unchecked cast
            return buffer.getPosition() - startPosition;
        } finally {
            writer.close();
        }
    }

    public static final DBEncoderFactory FACTORY = new DBEncoderFactory() {
        @Override
        public DBEncoder create() {
            return new DefaultDBEncoder();
        }
    };
}
