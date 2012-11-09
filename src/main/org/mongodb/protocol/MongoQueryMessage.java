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

import org.mongodb.ReadPreference;
import org.bson.io.OutputBuffer;
import org.mongodb.serialization.Serializer;

import java.util.Map;

public class MongoQueryMessage extends MongoRequestMessage {
    public MongoQueryMessage(String collectionName, int options, int numToSkip, int batchSize,
                             Map<String, Object> query, Map<String, Object> fields, ReadPreference readPref,
                             OutputBuffer buffer, Serializer serializer) {
        super(collectionName, query, options, readPref, buffer);

        int allOptions = options;
        if (readPref != null && readPref.isSlaveOk()) {
            allOptions |= QueryOptions.SLAVEOK;
        }

        queryOptions = allOptions;

        writeQuery(fields, numToSkip, batchSize, serializer);
        backpatchMessageLength();
    }

    public boolean hasOption(int option) {
        return (queryOptions & option) != 0;
    }


    private void writeQuery(final Map<String, Object> fields, final int numToSkip, final int batchSize,
                            final Serializer serializer) {
        buffer.writeInt(queryOptions);
        buffer.writeCString(collectionName);

        buffer.writeInt(numToSkip);
        buffer.writeInt(batchSize);

        addDocument(Map.class, query, serializer);
        if (fields != null)
            addDocument(Map.class, fields, serializer);
    }

    private final int queryOptions;
}
