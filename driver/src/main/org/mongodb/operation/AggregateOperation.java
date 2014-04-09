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

import org.mongodb.AggregationOptions;
import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.protocol.QueryResult;
import org.mongodb.session.Session;

import java.util.EnumSet;
import java.util.List;

import static org.mongodb.operation.OperationHelper.getConnectionProvider;

/**
 * An operation that executes an aggregation query
 *
 * @param <T> the type to deserialize the results to
 * @since 3.0
 */
public class AggregateOperation<T> extends AggregateBaseOperation<T> implements Operation<MongoCursor<T>> {
    public AggregateOperation(final MongoNamespace namespace, final List<Document> pipeline, final Decoder<T> decoder,
                              final AggregationOptions options, final ReadPreference readPreference) {
        super(namespace, pipeline, decoder, options, readPreference);
    }

    @SuppressWarnings("unchecked")
    @Override
    public MongoCursor<T> execute(final Session session) {
        CommandResult result = sendAndReceiveMessage(session);
        if (getOptions().getOutputMode() == AggregationOptions.OutputMode.INLINE) {
            return new InlineMongoCursor<T>(result.getAddress(), (List<T>) result.getResponse().get("result"));
        } else {
            int batchSize = getOptions().getBatchSize() == null ? 0 : getOptions().getBatchSize();
            return new MongoQueryCursor<T>(getNamespace(), createQueryResult(result), EnumSet.noneOf(QueryFlag.class), 0,
                                           batchSize, getDecoder(),
                                           getConnectionProvider(getReadPreference(), session));
        }
    }

    @SuppressWarnings("unchecked")
    private QueryResult<T> createQueryResult(final CommandResult result) {
        Document cursor = (Document) result.getResponse().get("cursor");
        long cursorId;
        List<T> results;
        if (cursor != null) {
            cursorId = cursor.getLong("id");
            results = (List<T>) cursor.get("firstBatch");
        } else {
            cursorId = 0;
            results = (List<T>) result.getResponse().get("result");
        }
        return new QueryResult<T>(results, cursorId, result.getAddress(), 0);
    }
}
