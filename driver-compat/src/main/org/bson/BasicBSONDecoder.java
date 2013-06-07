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

package org.bson;

import org.bson.io.BasicInputBuffer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Basic implementation of BSONDecoder interface that creates BasicBSONObject instances
 */
public class BasicBSONDecoder implements BSONDecoder {

    @Override
    public BSONObject readObject(final byte[] bytes) {
        final BSONCallback bsonCallback = new BasicBSONCallback();
        decode(bytes, bsonCallback);
        return (BSONObject) bsonCallback.get();
    }

    @Override
    public BSONObject readObject(final InputStream in) throws IOException {
        return readObject(readFully(in));
    }

    @Override
    public int decode(final byte[] bytes, final BSONCallback callback) {
        final BSONBinaryReader reader = new BSONBinaryReader(new BSONReaderSettings(), new BasicInputBuffer(ByteBuffer.wrap(bytes)), true);
        try {
            final BSONWriter writer = new BSONCallbackAdapter(new BSONWriterSettings(), callback);
            writer.pipe(reader);
            return reader.getBuffer().getPosition(); //TODO check this.
        } finally {
            reader.close();
        }
    }

    @Override
    public int decode(final InputStream in, final BSONCallback callback) throws IOException {
        return decode(readFully(in), callback);
    }

    private byte[] readFully(final InputStream input) throws IOException {
        byte[] buffer = new byte[8192];
        int bytesRead;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while ((bytesRead = input.read(buffer)) != -1) {
            output.write(buffer, 0, bytesRead);
        }
        return output.toByteArray();
    }
}
