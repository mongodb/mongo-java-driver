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
import org.bson.types.Document;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoQuery;
import org.mongodb.serialization.Serializer;

public class MongoQueryMessage extends MongoRequestMessage {

    public MongoQueryMessage(final String collectionName, final MongoFind find, final OutputBuffer buffer,
                             final Serializer<Document> serializer) {
        super(collectionName, find.getFilter().toMongoDocument(), find.getOptions(), find.getReadPreference(), buffer);

        init(find);
        addDocument(find.getFilter().toMongoDocument(), serializer);
        if (find.getFields() != null) {
            addDocument(find.getFields().toMongoDocument(), serializer);
        }
        backpatchMessageLength();
    }

    public MongoQueryMessage(final String collectionName, final MongoCommandOperation commandOperation,
                             final OutputBuffer buffer, final Serializer<Document> serializer) {
        super(collectionName, commandOperation.getCommand().toMongoDocument(), 0, commandOperation.getReadPreference(), buffer);

        init(commandOperation);
        addDocument(commandOperation.getCommand().toMongoDocument(), serializer);
        backpatchMessageLength();
    }

    private void init(final MongoQuery query) {
        int allOptions = 0;
        if (query.getReadPreference().isSlaveOk()) {
            allOptions |= QueryOptions.SLAVEOK;
        }

        writeQueryPrologue(allOptions, query);
    }

    private void writeQueryPrologue(final int queryOptions,
                                    final MongoQuery query) {
        buffer.writeInt(queryOptions);
        buffer.writeCString(collectionName);

        buffer.writeInt(query.getNumToSkip());
        buffer.writeInt(query.getBatchSize());
    }
}
