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
import org.bson.types.ObjectId;
import org.bson.types.Document;
import org.mongodb.operation.MongoRemove;
import org.mongodb.serialization.Serializer;

import java.util.Collection;

public class MongoDeleteMessage extends MongoRequestMessage {
    public MongoDeleteMessage(final String collectionName, final MongoRemove remove, final OutputBuffer buffer,
                              final Serializer<Document> serializer) {
        super(collectionName, OpCode.OP_DELETE, remove.getFilter().toMongoDocument(), buffer);
        writeDelete(serializer);
        backpatchMessageLength();
    }

    private void writeDelete(final Serializer<Document> serializer) {
        buffer.writeInt(0); // reserved
        buffer.writeCString(collectionName);

        final Collection<String> keys = query.keySet();

        if (keys.size() == 1 && keys.iterator().next().equals("_id")
                && query.get(keys.iterator().next()) instanceof ObjectId) {
            buffer.writeInt(1);
        }
        else {
            buffer.writeInt(0);
        }

        addDocument(query, serializer);
    }
}

