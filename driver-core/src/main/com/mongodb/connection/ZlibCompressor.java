/*
 * Copyright 2017 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoInternalException;
import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

class ZlibCompressor implements Compressor {
    @Override
    public String getName() {
        return "zlib";
    }

    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();  // TODO
            DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(baos);

            // TODO: optimized this loop
            int count = 0;
            for (ByteBuf cur : source) {
                while (cur.hasRemaining()) {
                    byte b = cur.get();
                    if (count >= 16) {
                        deflaterOutputStream.write(b);
                    }
                    count++;
                }
            }

            deflaterOutputStream.finish();
            target.writeBytes(baos.toByteArray());
        } catch (IOException e) {
            throw new MongoInternalException("", e);  // TODO
        }
    }


    @Override
    public void uncompress(final ByteBuf source, final ByteBuf target) {
        // TODO: avoid double copy
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(bais);

        try {
            int curByte = inflaterInputStream.read();
            while (curByte != -1) {
                target.put((byte) curByte);
                curByte = inflaterInputStream.read();
            }
        } catch (IOException e) {
            throw new MongoInternalException("", e);  // TODO
        }

    }

    @Override
    public byte getId() {
        return 2;
    }
}
