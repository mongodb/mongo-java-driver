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

package com.mongodb.connection;

import com.mongodb.MongoCompressor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

class ZlibCompressor extends Compressor {
    private final int level;

    ZlibCompressor(final MongoCompressor mongoCompressor) {
        this.level = mongoCompressor.getProperty(MongoCompressor.LEVEL, Deflater.DEFAULT_COMPRESSION);
    }

    @Override
    public String getName() {
        return "zlib";
    }

    @Override
    public byte getId() {
        return 2;
    }

    @Override
    InputStream getInputStream(final InputStream source) throws IOException {
        return new InflaterInputStream(source);
    }

    @Override
    OutputStream getOutputStream(final OutputStream source) throws IOException {
        return new DeflaterOutputStream(source, new Deflater(level));
    }
}
