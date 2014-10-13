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

import com.mongodb.MongoNamespace;
import com.mongodb.async.MongoFuture;
import com.mongodb.client.model.CreateIndexOptions;
import com.mongodb.client.model.RenameCollectionOptions;
import com.mongodb.operation.CreateIndexOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropIndexOperation;
import com.mongodb.operation.ListIndexesOperation;
import com.mongodb.operation.RenameCollectionOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.DocumentCodec;
import org.bson.types.Document;

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
    public MongoFuture<Void> createIndex(final Document key) {
        return createIndex(key, new CreateIndexOptions());
    }

    @Override
    public MongoFuture<Void> createIndex(final Document key, final CreateIndexOptions createIndexOptions) {
        return client.execute(new CreateIndexOperation(collectionNamespace, asBson(key))
                                  .name(createIndexOptions.getName())
                                  .background(createIndexOptions.isBackground())
                                  .unique(createIndexOptions.isUnique())
                                  .sparse(createIndexOptions.isSparse())
                                  .expireAfterSeconds(createIndexOptions.getExpireAfterSeconds())
                                  .version(createIndexOptions.getVersion())
                                  .weights(asBson((Document) createIndexOptions.getWeights()))
                                  .defaultLanguage(createIndexOptions.getDefaultLanguage())
                                  .languageOverride(createIndexOptions.getLanguageOverride())
                                  .textIndexVersion(createIndexOptions.getTextIndexVersion())
                                  .twoDSphereIndexVersion(createIndexOptions.getTwoDSphereIndexVersion())
                                  .bits(createIndexOptions.getBits())
                                  .min(createIndexOptions.getMin())
                                  .max(createIndexOptions.getMax())
                                  .bucketSize(createIndexOptions.getBucketSize()));
    }

    @Override
    public MongoFuture<List<Document>> getIndexes() {
        return client.execute(new ListIndexesOperation<Document>(collectionNamespace, new DocumentCodec()), primary());
    }

    @Override
    public MongoFuture<Void> drop() {
        return client.execute(new DropCollectionOperation(collectionNamespace));
    }

    @Override
    public MongoFuture<Void> dropIndex(final String indexName) {
        return client.execute(new DropIndexOperation(collectionNamespace, indexName));
    }

    @Override
    public MongoFuture<Void> dropIndexes() {
        return client.execute(new DropIndexOperation(collectionNamespace, "*"));
    }


    @Override
    public MongoFuture<Void> renameCollection(final MongoNamespace newCollectionNamespace) {
        return renameCollection(newCollectionNamespace, new RenameCollectionOptions());
    }

    @Override
    public MongoFuture<Void> renameCollection(final MongoNamespace newCollectionNamespace,
                                              final RenameCollectionOptions renameCollectionOptions) {
        return client.execute(new RenameCollectionOperation(collectionNamespace, newCollectionNamespace)
                                  .dropTarget(renameCollectionOptions.isDropTarget()));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private BsonDocument asBson(final Document document) {
        if (document == null) {
            return null;
        } else {
            return new BsonDocumentWrapper(document, new DocumentCodec());
        }

    }
}
