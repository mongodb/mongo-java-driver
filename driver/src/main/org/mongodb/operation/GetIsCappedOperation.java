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

import org.mongodb.Document;
import org.mongodb.Function;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.ReadBinding;
import org.mongodb.util.FieldHelpers;

import static org.mongodb.operation.OperationHelper.transformResult;

/**
 * Return if the collection is capped
 *
 * @since 3.0
 */
public class GetIsCappedOperation implements AsyncReadOperation<Boolean>, ReadOperation<Boolean> {

    private final MongoNamespace collectionNamespace;
    private final Document collStatsCommand;

    public GetIsCappedOperation(final MongoNamespace collectionNamespace) {
        this.collectionNamespace = collectionNamespace;
        collStatsCommand = new Document("collStats", collectionNamespace.getCollectionName());
    }

    @Override
    public Boolean execute(final ReadBinding binding) {
        return transformer().apply(new GetStatisticsOperation(collectionNamespace).execute(binding));
    }

    @Override
    public MongoFuture<Boolean> executeAsync(final AsyncReadBinding binding) {
        return transformResult(new GetStatisticsOperation(collectionNamespace).executeAsync(binding), transformer());
    }

    private Function<Document, Boolean> transformer() {
        return new Function<Document, Boolean>() {
            @Override
            public Boolean apply(final Document result) {
                return FieldHelpers.asBoolean(result.get("capped"));
            }
        };
    }
}
