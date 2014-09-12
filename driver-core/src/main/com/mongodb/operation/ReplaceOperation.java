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

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.protocol.ReplaceCommandProtocol;
import com.mongodb.protocol.ReplaceProtocol;
import com.mongodb.protocol.WriteCommandProtocol;
import com.mongodb.protocol.WriteProtocol;
import org.mongodb.BulkWriteResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An operation that atomically replaces a document in a collection with a new document.
 *
 * @since 3.0
 */
public class ReplaceOperation extends BaseWriteOperation {
    private final List<ReplaceRequest> replaceRequests;

    /**
     * Construct an instance.
     *
     * @param namespace the namespace
     * @param ordered whether the inserts are ordered
     * @param writeConcern the write concern to apply
     * @param replaceRequests the list of replace requests
     */
    public ReplaceOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                            final List<ReplaceRequest> replaceRequests) {
        super(namespace, ordered, writeConcern);
        this.replaceRequests = notNull("replace", replaceRequests);
    }

    /**
     * Get the replace requests.
     * @return the list of replace requests
     */
    public List<ReplaceRequest> getReplaceRequests() {
        return replaceRequests;
    }

    @Override
    protected WriteProtocol getWriteProtocol() {
        return new ReplaceProtocol(getNamespace(), isOrdered(), getWriteConcern(), replaceRequests);
    }

    @Override
    protected WriteCommandProtocol getCommandProtocol() {
        return new ReplaceCommandProtocol(getNamespace(), isOrdered(), getWriteConcern(), replaceRequests);
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.REPLACE;
    }

    @Override
    protected int getCount(final BulkWriteResult bulkWriteResult) {
        return bulkWriteResult.getMatchedCount() + bulkWriteResult.getUpserts().size();
    }

    @Override
    protected boolean getUpdatedExisting(final BulkWriteResult bulkWriteResult) {
        return bulkWriteResult.getMatchedCount() > 0;
    }
}
