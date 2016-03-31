/*
 * Copyright 2016 MongoDB, Inc.
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

package com.mongodb;

import org.bson.BSONCallback;
import org.bson.BSONObject;

import java.io.IOException;
import java.io.InputStream;

class SubsitutingDBDecoderFactory implements DBDecoderFactory {
    private final DBObject substituted;

    SubsitutingDBDecoderFactory(final DBObject substituted) {
        this.substituted = substituted;
    }

    @Override
    public DBDecoder create() {
        final DBDecoder defaultDecoder = DefaultDBDecoder.FACTORY.create();
        return new DBDecoder() {
            @Override
            public DBCallback getDBCallback(final DBCollection collection) {
                return defaultDecoder.getDBCallback(collection);
            }

            @Override
            public DBObject decode(final InputStream input, final DBCollection collection) throws IOException {
                defaultDecoder.decode(input, collection);
                return substituted;
            }

            @Override
            public DBObject decode(final byte[] bytes, final DBCollection collection) {
                return substituted;
            }

            @Override
            public BSONObject readObject(final byte[] bytes) {
                return substituted;
            }

            @Override
            public BSONObject readObject(final InputStream in) throws IOException {
                defaultDecoder.readObject(in);
                return substituted;
            }

            @Override
            public int decode(final byte[] bytes, final BSONCallback callback) {
                return defaultDecoder.decode(bytes, callback);
            }

            @Override
            public int decode(final InputStream in, final BSONCallback callback) throws IOException {
                throw new UnsupportedOperationException();
            }
        };
    }
}
