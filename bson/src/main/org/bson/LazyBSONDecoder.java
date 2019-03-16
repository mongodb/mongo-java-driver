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

package org.bson;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 *  A decoder for {@code LazyBSONObject} instances.
 */
public class LazyBSONDecoder implements BSONDecoder {
    private static final int BYTES_IN_INTEGER = 4;

    @Override
    public BSONObject readObject(final byte[] bytes) {
        BSONCallback bsonCallback = new LazyBSONCallback();
        decode(bytes, bsonCallback);
        return (BSONObject) bsonCallback.get();
    }

    @Override
    public BSONObject readObject(final InputStream in) throws IOException {
        BSONCallback bsonCallback = new LazyBSONCallback();
        decode(in, bsonCallback);
        return (BSONObject) bsonCallback.get();
    }

    @Override
    public int decode(final byte[] bytes, final BSONCallback callback) {
        try {
            return decode(new ByteArrayInputStream(bytes), callback);
        } catch (IOException e) {
            throw new BSONException("Invalid bytes received", e);
        }
    }

    @Override
    public int decode(final InputStream in, final BSONCallback callback) throws IOException {
        byte[] documentSizeBuffer = new byte[BYTES_IN_INTEGER];
        int documentSize = Bits.readInt(in, documentSizeBuffer);
        byte[] documentBytes = Arrays.copyOf(documentSizeBuffer, documentSize);
        Bits.readFully(in, documentBytes, BYTES_IN_INTEGER, documentSize - BYTES_IN_INTEGER);

        // note that we are handing off ownership of the documentBytes byte array to the callback
        callback.gotBinary(null, (byte) 0, documentBytes);
        return documentSize;
    }
}
