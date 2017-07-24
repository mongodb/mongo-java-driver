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

import com.mongodb.bulk.DeleteRequest;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.io.BsonOutput;

import java.util.List;

/**
 * An OP_DELETE message.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-delete OP_DELETE
 */
class DeleteMessage extends RequestMessage {
    private final List<DeleteRequest> deleteRequests;

    DeleteMessage(final String collectionName, final List<DeleteRequest> deletes, final MessageSettings settings) {
        super(collectionName, OpCode.OP_DELETE, settings);
        this.deleteRequests = deletes;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final int messageStartPosition) {
        DeleteRequest deleteRequest = deleteRequests.get(0);
        bsonOutput.writeInt32(0); // reserved
        bsonOutput.writeCString(getCollectionName());

        if (deleteRequest.isMulti()) {
            bsonOutput.writeInt32(0);
        } else {
            bsonOutput.writeInt32(1);
        }

        int firstDocumentStartPosition = bsonOutput.getPosition();

        addDocument(deleteRequest.getFilter(), bsonOutput, new NoOpFieldNameValidator());
        if (deleteRequests.size() == 1) {
            return new EncodingMetadata(null, firstDocumentStartPosition);
        } else {
            return new EncodingMetadata(new DeleteMessage(getCollectionName(), deleteRequests.subList(1, deleteRequests.size()),
                                                          getSettings()),
                                        firstDocumentStartPosition);
        }
    }

}

