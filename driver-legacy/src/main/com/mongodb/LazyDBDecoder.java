/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.LazyBSONDecoder;

import java.io.IOException;
import java.io.InputStream;

/**
 *  A decoder for {@code LazyDBObject} instances.
 */
public class LazyDBDecoder extends LazyBSONDecoder implements DBDecoder {

    @Override
    public DBCallback getDBCallback(final DBCollection collection) {
        // callback doesn't do anything special, could be unique per decoder
        // but have to create per collection due to DBRef, at least
        return new LazyDBCallback(collection);
    }

    @Override
    public DBObject readObject(final InputStream in) throws IOException {
        DBCallback dbCallback = getDBCallback(null);
        decode(in, dbCallback);
        return (DBObject) dbCallback.get();
    }

    @Override
    public DBObject decode(final InputStream input, final DBCollection collection) throws IOException {
        DBCallback callback = getDBCallback(collection);
        decode(input, callback);
        return (DBObject) callback.get();
    }

    @Override
    public DBObject decode(final byte[] bytes, final DBCollection collection) {
        DBCallback callback = getDBCallback(collection);
        decode(bytes, callback);
        return (DBObject) callback.get();
    }

    public static final DBDecoderFactory FACTORY = new DBDecoderFactory() {
        @Override
        public DBDecoder create() {
            return new LazyDBDecoder();
        }
    };
}
