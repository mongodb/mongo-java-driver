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
import org.mongodb.operation.MongoRemove;
import org.mongodb.serialization.Serializer;

import java.util.Collection;

public class MongoDeleteMessage extends MongoRequestMessage {
    public MongoDeleteMessage(final String collectionName, final MongoRemove remove, final OutputBuffer buffer,
                              final Serializer<Document> serializer) {
        super(collectionName, OpCode.OP_DELETE, buffer);
        writeDelete(remove, serializer);
        backpatchMessageLength();
    }

    private void writeDelete(final MongoRemove remove, final Serializer<Document> serializer) {
        getBuffer().writeInt(0); // reserved
        getBuffer().writeCString(getCollectionName());

        final Document queryFilterDocument = remove.getFilter().toDocument();

        final Collection<String> keys = queryFilterDocument.keySet();

        if (remove.isMulti()) {
            getBuffer().writeInt(0);
        }
        else {
            getBuffer().writeInt(0);
        }

        addDocument(queryFilterDocument, serializer);
    }
}

