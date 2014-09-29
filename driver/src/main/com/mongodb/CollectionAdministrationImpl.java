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

package com.mongodb;

import com.mongodb.client.CollectionAdministration;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.operation.CreateIndexesOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.Index;
import com.mongodb.operation.OperationExecutor;
import org.mongodb.Document;

import java.util.List;

import static com.mongodb.ReadPreference.primary;

/**
 * Encapsulates functionality that is not part of the day-to-day use of a Collection.  For example, via this admin class you can create
 * indexes and drop the collection.
 */
class CollectionAdministrationImpl implements CollectionAdministration {

    private final MongoNamespace collectionNamespace;
    private final OperationExecutor operationExecutor;


    CollectionAdministrationImpl(final MongoNamespace collectionNamespace,
                                 final OperationExecutor operationExecutor) {
        this.collectionNamespace = collectionNamespace;
        this.operationExecutor = operationExecutor;
    }

    @Override
    public void createIndexes(final List<Index> indexes) {
        operationExecutor.execute(new CreateIndexesOperation(collectionNamespace, indexes));
    }

    @Override
    public List<Document> getIndexes() {
        return operationExecutor.execute(new ListIndexesOperation<Document>(collectionNamespace, new DocumentCodec()), primary());
    }

    @Override
    public void drop() {
        operationExecutor.execute(new DropCollectionOperation(collectionNamespace));
    }

    @Override
    public void dropIndex(final Index index) {
        operationExecutor.execute(new DropIndexOperation(collectionNamespace, index.getName()));
    }

    @Override
    public void dropIndexes() {
        operationExecutor.execute(new DropIndexOperation(collectionNamespace, "*"));
    }
}
