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

import com.mongodb.bulk.UpdateRequest;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import org.bson.io.BsonOutput;

import java.util.List;

import static com.mongodb.bulk.WriteRequest.Type.REPLACE;

/**
 * An OP_UPDATE message.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-update OP_UPDATE
 */
class UpdateMessage extends RequestMessage {
    private final List<UpdateRequest> updates;

    UpdateMessage(final String collectionName, final List<UpdateRequest> updates, final MessageSettings settings) {
        super(collectionName, OpCode.OP_UPDATE, settings);
        this.updates = updates;
    }

    public List<UpdateRequest> getUpdateRequests() {
        return updates;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput, final int messageStartPosition) {
        bsonOutput.writeInt32(0); // reserved
        bsonOutput.writeCString(getCollectionName());

        UpdateRequest updateRequest = updates.get(0);
        int flags = 0;
        if (updateRequest.isUpsert()) {
            flags |= 1;
        }
        if (updateRequest.isMulti()) {
            flags |= 2;
        }
        bsonOutput.writeInt32(flags);

        int firstDocumentStartPosition = bsonOutput.getPosition();

        addDocument(updateRequest.getFilter(), bsonOutput, new NoOpFieldNameValidator());
        if (updateRequest.getType() == REPLACE) {
            addCollectibleDocument(updateRequest.getUpdate(), bsonOutput, new CollectibleDocumentFieldNameValidator());
        } else {
            int bufferPosition = bsonOutput.getPosition();
            addDocument(updateRequest.getUpdate(), bsonOutput, new UpdateFieldNameValidator());
            if (bsonOutput.getPosition() == bufferPosition + 5) {
                throw new IllegalArgumentException("Invalid BSON document for an update");
            }
        }

        if (updates.size() == 1) {
            return new EncodingMetadata(null, firstDocumentStartPosition);
        } else {
            return new EncodingMetadata(new UpdateMessage(getCollectionName(), updates.subList(1, updates.size()), getSettings()),
                                    firstDocumentStartPosition);
        }
    }

}
