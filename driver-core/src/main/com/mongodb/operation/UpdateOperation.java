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
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.protocol.UpdateCommandProtocol;
import com.mongodb.protocol.UpdateProtocol;
import com.mongodb.protocol.WriteCommandProtocol;
import com.mongodb.protocol.WriteProtocol;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An operation that updates a document in a collection.
 *
 * @since 3.0
 */
public class UpdateOperation extends BaseWriteOperation {
    private final List<UpdateRequest> updates;

    /**
     * Construct an instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param ordered whether the updates are ordered.
     * @param writeConcern the write concern for the operation.
     * @param updates the update requests.
     */
    public UpdateOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                           final List<UpdateRequest> updates) {
        super(namespace, ordered, writeConcern);
        this.updates = notNull("update", updates);
    }

    /**
     * Gets the list of update requests.
     *
     * @return the update requests
     */
    public List<UpdateRequest> getUpdateRequests() {
        return updates;
    }

    @Override
    protected WriteProtocol getWriteProtocol() {
        return new UpdateProtocol(getNamespace(), isOrdered(), getWriteConcern(), updates);
    }

    @Override
    protected WriteCommandProtocol getCommandProtocol() {
        return new UpdateCommandProtocol(getNamespace(), isOrdered(), getWriteConcern(), updates);
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.UPDATE;
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
