/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.protocol.message;

import com.mongodb.operation.RemoveRequest;
import org.bson.io.BsonOutputStream;

import java.util.List;

public class DeleteMessage extends RequestMessage {
    private final List<RemoveRequest> removeRequests;

    public DeleteMessage(final String collectionName, final List<RemoveRequest> removeRequests,
                         final MessageSettings settings) {
        super(collectionName, OpCode.OP_DELETE, settings);
        this.removeRequests = removeRequests;
    }

    @Override
    protected RequestMessage encodeMessageBody(final BsonOutputStream outputStream, final int messageStartPosition) {
        writeDelete(removeRequests.get(0), outputStream);
        if (removeRequests.size() == 1) {
            return null;
        } else {
            return new DeleteMessage(getCollectionName(), removeRequests.subList(1, removeRequests.size()), getSettings());
        }
    }

    private void writeDelete(final RemoveRequest removeRequest, final BsonOutputStream outputStream) {
        outputStream.writeInt32(0); // reserved
        outputStream.writeCString(getCollectionName());

        if (removeRequest.isMulti()) {
            outputStream.writeInt32(0);
        } else {
            outputStream.writeInt32(1);
        }

        addDocument(removeRequest.getCriteria(), getBsonDocumentCodec(), outputStream, new NoOpFieldNameValidator());
    }
}

