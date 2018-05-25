/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import com.mongodb.bulk.DeleteRequest;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.io.BsonOutput;

/**
 * An OP_DELETE message.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-delete OP_DELETE
 */
class DeleteMessage extends LegacyMessage {
    private final DeleteRequest deleteRequest;

    DeleteMessage(final String collectionName, final DeleteRequest deleteRequest, final MessageSettings settings) {
        super(collectionName, OpCode.OP_DELETE, settings);
        this.deleteRequest = deleteRequest;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput) {
        bsonOutput.writeInt32(0); // reserved
        bsonOutput.writeCString(getCollectionName());

        if (deleteRequest.isMulti()) {
            bsonOutput.writeInt32(0);
        } else {
            bsonOutput.writeInt32(1);
        }

        int firstDocumentStartPosition = bsonOutput.getPosition();

        addDocument(deleteRequest.getFilter(), bsonOutput, new NoOpFieldNameValidator());
        return new EncodingMetadata(firstDocumentStartPosition);
    }
}

