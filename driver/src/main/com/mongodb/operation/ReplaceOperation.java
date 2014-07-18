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

import com.mongodb.WriteConcern;
import com.mongodb.protocol.ReplaceCommandProtocol;
import com.mongodb.protocol.ReplaceProtocol;
import com.mongodb.protocol.WriteCommandProtocol;
import com.mongodb.protocol.WriteProtocol;
import org.bson.codecs.Encoder;
import org.mongodb.BulkWriteResult;
import org.mongodb.MongoNamespace;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An operation that atomically replaces a document in a collection with a new document.
 *
 * @param <T> the document type
 * @since 3.0
 */
public class ReplaceOperation<T> extends BaseWriteOperation {
    private final List<ReplaceRequest<T>> replaceRequests;
    private final Encoder<T> encoder;

    public ReplaceOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                            final List<ReplaceRequest<T>> replaceRequests, final Encoder<T> encoder) {
        super(namespace, ordered, writeConcern);
        this.replaceRequests = notNull("replace", replaceRequests);
        this.encoder = notNull("encoder", encoder);
    }

    @Override
    protected WriteProtocol getWriteProtocol() {
        return new ReplaceProtocol<T>(getNamespace(), isOrdered(), getWriteConcern(), replaceRequests, encoder);
    }

    @Override
    protected WriteCommandProtocol getCommandProtocol() {
        return new ReplaceCommandProtocol<T>(getNamespace(), isOrdered(), getWriteConcern(), replaceRequests, encoder);
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
