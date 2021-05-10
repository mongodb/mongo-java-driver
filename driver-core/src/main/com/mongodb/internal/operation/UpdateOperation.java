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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.internal.ClientSideOperationTimeoutFactory;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
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
     * @param clientSideOperationTimeoutFactory the client side operation factory
     * @param namespace     the database and collection namespace for the operation.
     * @param ordered       whether the updates are ordered.
     * @param writeConcern  the write concern for the operation.
     * @param updates       the update requests.
     */
    public UpdateOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final MongoNamespace namespace,
                           final boolean ordered, final WriteConcern writeConcern, final List<UpdateRequest> updates) {
        this(clientSideOperationTimeoutFactory, namespace, ordered, writeConcern, false, updates);
    }

    /**
     * Construct an instance.
     *
     * @param clientSideOperationTimeoutFactory the client side operation timeout factory
     * @param namespace the database and collection namespace for the operation.
     * @param ordered whether the updates are ordered.
     * @param writeConcern the write concern for the operation.
     * @param retryWrites   if writes should be retried if they fail due to a network error.
     * @param updates the update requests.
     * @since 3.6
     */
    public UpdateOperation(final ClientSideOperationTimeoutFactory clientSideOperationTimeoutFactory, final MongoNamespace namespace,
                           final boolean ordered, final WriteConcern writeConcern, final boolean retryWrites,
                           final List<UpdateRequest> updates) {
        super(clientSideOperationTimeoutFactory, namespace, ordered, writeConcern, retryWrites);
        this.updates = notNull("update", updates);
        isTrueArgument("updateRequests not empty", !updates.isEmpty());
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
    protected List<? extends WriteRequest> getWriteRequests() {
        return getUpdateRequests();
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.UPDATE;
    }

}
