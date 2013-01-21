/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

package org.mongodb.protocol;

import org.bson.io.OutputBuffer;
import org.mongodb.WriteConcern;
import org.mongodb.operation.MongoInsert;
import org.mongodb.serialization.Serializer;

public class MongoInsertMessage<T> extends MongoRequestMessage {
    public MongoInsertMessage(final String collectionName, final MongoInsert<T> insert, final OutputBuffer buffer,
                              final Serializer<T> serializer) {
        super(collectionName, OpCode.OP_INSERT, buffer);
        writeInsertPrologue(insert.getWriteConcern());
        backpatchMessageLength();
        for (final T document : insert.getDocuments()) {
            addDocument(document, serializer);
        }
    }

    private void writeInsertPrologue(final WriteConcern concern) {
        int flags = 0;
        if (concern.getContinueOnErrorForInsert()) {
            flags |= 1;
        }
        getBuffer().writeInt(flags);
        getBuffer().writeCString(getCollectionName());
    }
}
