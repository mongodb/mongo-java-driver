/**
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
 *
 */

package org.mongodb.protocol;

import org.bson.io.OutputBuffer;
import org.mongodb.serialization.Serializer;

import java.util.Map;

public class MongoUpdateMessage extends MongoRequestMessage {
    MongoUpdateMessage(String collectionName, boolean upsert, boolean multi,
                       Map<String, Object> query,
                       Map<String, Object> updateOperations, OutputBuffer buffer, Serializer serializer) {
        super(collectionName, OpCode.OP_UPDATE, query, buffer);
        writeUpdate(upsert, multi, query, updateOperations, serializer);
    }

    void writeUpdate(final boolean upsert, final boolean multi, final Map<String, Object> query,
                     final Map<String, Object> updateOperations, final Serializer serializer) {
        buffer.writeInt(0); // reserved
        buffer.writeCString(collectionName);

        int flags = 0;
        if (upsert) flags |= 1;
        if (multi) flags |= 2;
        buffer.writeInt(flags);

        addDocument(query.getClass(), query, serializer);
        addDocument(updateOperations.getClass(), updateOperations, serializer);
    }
}
