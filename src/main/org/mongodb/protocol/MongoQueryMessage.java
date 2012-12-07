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
import org.mongodb.MongoDocument;
import org.mongodb.operation.MongoQuery;
import org.mongodb.serialization.Serializer;

import java.util.Map;

public class MongoQueryMessage extends MongoRequestMessage {

    public MongoQueryMessage(String collectionName, MongoQuery query,
                             OutputBuffer buffer, Serializer serializer) {
        super(collectionName, query.getFilter().asDocument(), query.getOptions(), query.getReadPreference(), buffer);

        int allOptions = query.getOptions();
        if (query.getReadPreference().isSlaveOk()) {
            allOptions |= QueryOptions.SLAVEOK;
        }

        queryOptions = allOptions;

        writeQuery(query, serializer);
        backpatchMessageLength();
    }

    public boolean hasOption(int option) {
        return (queryOptions & option) != 0;
    }


    private void writeQuery(final MongoQuery query,
                            final Serializer serializer) {
        buffer.writeInt(queryOptions);
        buffer.writeCString(collectionName);

        buffer.writeInt(query.getNumToSkip());
        buffer.writeInt(query.getBatchSize());

        addDocument(MongoDocument.class, query.getFilter().asDocument(), serializer);
        if (query.getFields() != null)
            addDocument(Map.class, query.getFields().asDocument(), serializer);
    }

    private final int queryOptions;
}
