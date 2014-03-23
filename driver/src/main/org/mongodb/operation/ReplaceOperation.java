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
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.protocol.ReplaceCommandProtocol;
import org.mongodb.protocol.ReplaceProtocol;
import org.mongodb.protocol.WriteCommandProtocol;
import org.mongodb.protocol.WriteProtocol;

import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;

public class ReplaceOperation<T> extends BaseWriteOperation {
    private final List<ReplaceRequest<T>> replaceRequests;
    private final Encoder<Document> queryEncoder;
    private final Encoder<T> encoder;

    public ReplaceOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                            final List<ReplaceRequest<T>> replaceRequests, final Encoder<Document> queryEncoder,
                            final Encoder<T> encoder) {
        super(namespace, ordered, writeConcern);
        this.replaceRequests = notNull("replace", replaceRequests);
        this.queryEncoder = notNull("queryEncoder", queryEncoder);
        this.encoder = notNull("encoder", encoder);
    }

    @Override
    protected WriteProtocol getWriteProtocol() {
        return new ReplaceProtocol<T>(getNamespace(), isOrdered(), getWriteConcern(), replaceRequests, queryEncoder, encoder);
    }

    @Override
    protected WriteCommandProtocol getCommandProtocol() {
        return new ReplaceCommandProtocol<T>(getNamespace(), isOrdered(), getWriteConcern(), replaceRequests, queryEncoder, encoder);
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.REPLACE;
    }

    @Override
    protected int getCount(final BulkWriteResult bulkWriteResult) {
        return bulkWriteResult.getUpdatedCount() + bulkWriteResult.getUpserts().size();
    }

    @Override
    protected boolean getUpdatedExisting(final BulkWriteResult bulkWriteResult) {
        return bulkWriteResult.getUpdatedCount() > 0 && bulkWriteResult.getUpserts().isEmpty();
    }
}
