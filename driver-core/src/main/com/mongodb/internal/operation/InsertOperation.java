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
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.WriteRequest;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An operation that inserts one or more documents into a collection.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class InsertOperation extends BaseWriteOperation {
    private final List<InsertRequest> insertRequests;

    public InsertOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                           final boolean retryWrites, final List<InsertRequest> insertRequests) {
        super(namespace, ordered, writeConcern, retryWrites);
        this.insertRequests = notNull("insertRequests", insertRequests);
        isTrueArgument("insertRequests not empty", !insertRequests.isEmpty());
    }


    public List<InsertRequest> getInsertRequests() {
        return insertRequests;
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.INSERT;
    }

    @Override
    protected List<? extends WriteRequest> getWriteRequests() {
        return getInsertRequests();
    }

}
