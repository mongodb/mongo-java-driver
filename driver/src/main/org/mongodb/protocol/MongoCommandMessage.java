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

package org.mongodb.protocol;

import org.mongodb.Document;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.command.MongoCommand;
import org.mongodb.serialization.Serializer;

public class MongoCommandMessage extends MongoQueryBaseMessage {
    private final MongoCommand commandOperation;
    private final Serializer<Document> serializer;

    public MongoCommandMessage(final String collectionName, final MongoCommand commandOperation, final Serializer<Document> serializer) {
        super(collectionName);
        this.commandOperation = commandOperation;
        this.serializer = serializer;
    }

    @Override
    protected void serializeMessageBody(final ChannelAwareOutputBuffer buffer) {
        writeQueryPrologue(commandOperation, buffer);
        addDocument(commandOperation.toDocument(), serializer, buffer);
    }
}
