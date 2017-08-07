/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.FieldNameValidator;
import org.bson.io.BsonOutput;

/**
 * A command message that uses OP_QUERY to send the command.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
class SimpleCommandMessage extends CommandMessage {
    private final boolean slaveOk;
    private final BsonDocument command;
    private final FieldNameValidator validator;

    SimpleCommandMessage(final String collectionName, final BsonDocument command, final boolean slaveOk, final MessageSettings settings) {
        this(collectionName, command, slaveOk, new NoOpFieldNameValidator(), settings);
    }

    SimpleCommandMessage(final String collectionName, final BsonDocument command, final boolean slaveOk,
                         final FieldNameValidator validator, final MessageSettings settings) {
        super(collectionName, useNewOpCode(settings) ? OpCode.OP_MSG : OpCode.OP_QUERY, settings);
        this.slaveOk = slaveOk;
        this.command = command;
        this.validator = validator;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final int messageStartPosition) {
        if (useNewOpCode()) {
            bsonOutput.writeInt32(0);  // flag bits
            bsonOutput.writeByte(0);   // payload type
            // TODO: this is ugly
            command.put("$db", new BsonString(new MongoNamespace(getCollectionName()).getDatabaseName()));
        } else {
            bsonOutput.writeInt32(slaveOk ? 1 << 2 : 0);
            bsonOutput.writeCString(getCollectionName());
            bsonOutput.writeInt32(0);
            bsonOutput.writeInt32(-1);
        }

        int firstDocumentPosition = bsonOutput.getPosition();
        addDocument(command, bsonOutput, validator);
        return new EncodingMetadata(null, firstDocumentPosition);
    }

    private boolean useNewOpCode() {
        return useNewOpCode(getSettings());
    }

    private static boolean useNewOpCode(final MessageSettings settings) {
        return settings.getServerVersion().compareTo(new ServerVersion(3, 5)) >= 0;
    }
}
