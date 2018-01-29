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

import org.bson.BSONDecoder;

import java.io.IOException;
import java.io.InputStream;

/**
 * An interface for decoders of BSON into instances of DBObject that belong to a DBCollection.
 */
public interface DBDecoder extends BSONDecoder {

    /**
     * Get a callback for the given collection.
     *
     * @param collection the collection
     * @return the callback
     */
    DBCallback getDBCallback(DBCollection collection);

    /**
     * Decode a single DBObject belonging to the given collection from the given input stream.
     *
     * @param input      the input stream
     * @param collection the collection
     * @return the DBObject
     * @throws IOException may throw an exception while decoding from the {@code InputStream}
     */
    DBObject decode(InputStream input, DBCollection collection) throws IOException;

    /**
     * Decode a single DBObject belonging to the given collection from the given array of bytes.
     *
     * @param bytes      the byte array
     * @param collection the collection
     * @return the DBObject
     */
    DBObject decode(byte[] bytes, DBCollection collection);
}
