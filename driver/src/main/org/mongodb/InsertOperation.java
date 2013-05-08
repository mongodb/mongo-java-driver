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

package org.mongodb;

import org.mongodb.io.BufferPool;
import org.mongodb.operation.MongoInsert;
import org.mongodb.protocol.MongoInsertMessage;
import org.mongodb.protocol.MongoRequestMessage;

import java.nio.ByteBuffer;

public class InsertOperation<T> extends WriteOperation {
    private final MongoInsert<T> insert;
    private final Encoder<T> encoder;

    public InsertOperation(final MongoNamespace namespace, final MongoInsert<T> insert, final Encoder<T> encoder,
                           final BufferPool<ByteBuffer> bufferPool) {
        super(namespace, bufferPool);
        this.insert = insert;
        this.encoder = encoder;
    }

    protected MongoRequestMessage createRequestMessage() {
        return new MongoInsertMessage<T>(getNamespace().getFullName(), insert, encoder);
    }

    public MongoInsert<T> getWrite() {
        return insert;
    }
}
