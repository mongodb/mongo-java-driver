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

package com.mongodb.async.client;

import com.mongodb.async.MongoFuture;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.operation.CreateIndexesOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.GetIndexesOperation;
import com.mongodb.operation.Index;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;

import java.util.List;

import static com.mongodb.ReadPreference.primary;

/**
 * Provides the functionality for a collection that is useful for administration, but not necessarily in the course of normal use of a
 * collection.
 *
 * @since 3.0
 */
public class CollectionAdministrationImpl implements CollectionAdministration {

    private final MongoClientImpl client;
    private final MongoNamespace collectionNamespace;


    CollectionAdministrationImpl(final MongoClientImpl client,
                                 final MongoNamespace collectionNamespace) {
        this.client = client;
        this.collectionNamespace = collectionNamespace;
    }

    @Override
    public MongoFuture<Void> createIndexes(final List<Index> indexes) {
        return client.execute(new CreateIndexesOperation(indexes, collectionNamespace));
    }

    @Override
    public MongoFuture<List<Document>> getIndexes() {
        return client.execute(new GetIndexesOperation<Document>(collectionNamespace, new DocumentCodec()), primary());
    }

    @Override
    public MongoFuture<Void> drop() {
        return client.execute(new DropCollectionOperation(collectionNamespace));
    }

    @Override
    public MongoFuture<Void> dropIndex(final Index index) {
        return client.execute(new DropIndexOperation(collectionNamespace, index.getName()));
    }

    @Override
    public MongoFuture<Void> dropIndexes() {
        return client.execute(new DropIndexOperation(collectionNamespace, "*"));
    }
}
