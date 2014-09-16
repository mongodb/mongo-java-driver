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

import com.mongodb.operation.BaseUpdateRequest;
import com.mongodb.operation.ReplaceRequest;
import org.bson.io.BsonOutputStream;

import java.util.List;

/**
 * An update message that handles full document replacements.
 *
 * @since 3.0
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-update OP_UPDATE
 */
public class ReplaceMessage extends BaseUpdateMessage {
    private final List<ReplaceRequest> replaceRequests;

    /**
     * Construct an instance.
     *
     * @param collectionName the full name of the collection
     * @param replaceRequests the list of replace requests
     * @param settings the message settings
     */
    public ReplaceMessage(final String collectionName, final List<ReplaceRequest> replaceRequests, final MessageSettings settings) {
        super(collectionName, OpCode.OP_UPDATE, settings);
        this.replaceRequests = replaceRequests;
    }

    @Override
    protected RequestMessage encodeMessageBody(final BsonOutputStream outputStream, final int messageStartPosition) {
        writeBaseUpdate(outputStream);
        addCollectibleDocument(replaceRequests.get(0).getReplacement(), outputStream, new CollectibleDocumentFieldNameValidator());
        if (replaceRequests.size() == 1) {
            return null;
        } else {
            return new ReplaceMessage(getCollectionName(), replaceRequests.subList(1, replaceRequests.size()), getSettings());
        }
    }

    @Override
    protected BaseUpdateRequest getUpdateBase() {
        return replaceRequests.get(0);
    }
}
