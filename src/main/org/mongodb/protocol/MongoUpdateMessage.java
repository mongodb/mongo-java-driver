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
import org.mongodb.operation.MongoUpdate;
import org.mongodb.serialization.Serializer;

public class MongoUpdateMessage extends MongoRequestMessage {
    public MongoUpdateMessage(String collectionName, MongoUpdate update, OutputBuffer buffer, Serializer serializer) {
        super(collectionName, OpCode.OP_UPDATE, update.getFilter().toMongoDocument(), buffer);
        writeUpdate(update, serializer);
        backpatchMessageLength();
    }

    void writeUpdate(MongoUpdate update, final Serializer serializer) {
        buffer.writeInt(0); // reserved
        buffer.writeCString(collectionName);

        int flags = 0;
        if (update.isUpsert()) {
            flags |= 1;
        }
        if (update.isMulti()) {
            flags |= 2;
        }
        buffer.writeInt(flags);

        addDocument(query.getClass(), query, serializer);
        addDocument(update.getUpdateOperations().getClass(), update.getUpdateOperations(), serializer);
    }
}
