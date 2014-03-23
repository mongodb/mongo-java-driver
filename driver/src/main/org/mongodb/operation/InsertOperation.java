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

package org.mongodb.operation;

import org.mongodb.BulkWriteResult;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.protocol.InsertCommandProtocol;
import org.mongodb.protocol.InsertProtocol;
import org.mongodb.protocol.WriteCommandProtocol;
import org.mongodb.protocol.WriteProtocol;

import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

public class InsertOperation<T> extends BaseWriteOperation {
    private final List<InsertRequest<T>> insertRequestList;
    private final Encoder<T> encoder;

    public InsertOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                           final List<InsertRequest<T>> insertRequestList, final Encoder<T> encoder) {
        super(namespace, ordered, writeConcern);
        this.insertRequestList = notNull("insertList", insertRequestList);
        this.encoder = notNull("encoder", encoder);
    }

    @Override
    protected WriteProtocol getWriteProtocol() {
        return new InsertProtocol<T>(getNamespace(), isOrdered(), getWriteConcern(), insertRequestList, encoder);
    }

    @Override
    protected WriteCommandProtocol getCommandProtocol() {
        return new InsertCommandProtocol<T>(getNamespace(), isOrdered(), getWriteConcern(), insertRequestList, encoder);
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.INSERT;
    }

    @Override
    protected int getCount(final BulkWriteResult bulkWriteResult) {
        return 0;
    }
}
