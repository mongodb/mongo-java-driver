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
import com.mongodb.protocol.DeleteCommandProtocol;
import com.mongodb.protocol.DeleteProtocol;
import com.mongodb.protocol.WriteCommandProtocol;
import com.mongodb.protocol.WriteProtocol;
import org.mongodb.BulkWriteResult;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An operation that removes one or more documents from a collection.
 *
 * @since 3.0
 */
public class RemoveOperation extends BaseWriteOperation {
    private final List<RemoveRequest> removeRequests;

    public RemoveOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                           final List<RemoveRequest> removeRequests) {
        super(namespace, ordered, writeConcern);
        this.removeRequests = notNull("removes", removeRequests);
    }

    @Override
    protected WriteProtocol getWriteProtocol() {
        return new DeleteProtocol(getNamespace(), isOrdered(), getWriteConcern(), removeRequests);
    }

    @Override
    protected WriteCommandProtocol getCommandProtocol() {
        return new DeleteCommandProtocol(getNamespace(), isOrdered(), getWriteConcern(), removeRequests);
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.REMOVE;
    }

    @Override
    protected int getCount(final BulkWriteResult bulkWriteResult) {
        return bulkWriteResult.getRemovedCount();
    }
}
