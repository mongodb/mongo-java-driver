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

import org.bson.ByteBuf;
import org.bson.io.BsonOutput;

import java.io.ByteArrayOutputStream;
import java.util.List;

class NoOpCompessor implements Compressor {
    @Override
    public String getName() {
        return "noop";
    }

    @Override
    public void compress(final List<ByteBuf> source, final BsonOutput target) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();  // TODO

        // TODO: this is wonky to skip the MsgHeader here
        int count = 0;
        for (ByteBuf cur : source) {
            while (cur.hasRemaining()) {
                byte b = cur.get();
                if (count >= 16) {
                    baos.write(b);
                }
                count++;
            }
        }

        target.writeBytes(baos.toByteArray());
    }

    @Override
    public byte getId() {
        return 0;
    }

    @Override
    public void uncompress(final ByteBuf source, final ByteBuf target) {
        while (source.hasRemaining()) {
            target.put(source.get());
        }
    }
}
