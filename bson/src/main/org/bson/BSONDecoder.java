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

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface for decoders of BSON documents.
 */
public interface BSONDecoder {

    /**
     * Read a single BSON object from the given bytes.
     *
     * @param bytes the bytes in BSON format
     * @return the BSON object for the given bytes
     */
    BSONObject readObject(byte[] bytes);

    /**
     * Read a single BSON object from the given input stream.
     *
     * @param in the input stream in BSON format
     * @return the BSON object for the given bytes
     * @throws java.io.IOException if there's a problem reading the object from the {@code InputStream}
     */
    BSONObject readObject(InputStream in) throws IOException;

    /**
     * Decode a single BSON object into the given callback from the given byte array.
     *
     * @param bytes the bytes in BSON format
     * @param callback the callback
     * @return the number of bytes in the BSON object
     */
    int decode(byte[] bytes, BSONCallback callback);

    /**
     * Decode a single BSON object into the given callback from the given input stream.
     *
     * @param in the input stream in BSON format
     * @param callback the callback
     * @return the number of bytes read from the input stream
     * @throws java.io.IOException if there's a problem reading from the {@code InputStream}
     */
    int decode(InputStream in, BSONCallback callback) throws IOException;
}
