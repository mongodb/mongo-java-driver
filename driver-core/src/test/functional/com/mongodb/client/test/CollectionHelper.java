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

package com.mongodb.client.test;

import com.mongodb.CommandFailureException;
import com.mongodb.MongoCursor;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.codecs.DocumentCodec;
import com.mongodb.operation.CountOperation;
import com.mongodb.operation.CreateCollectionOperation;
import com.mongodb.operation.CreateCollectionOptions;
import com.mongodb.operation.CreateIndexesOperation;
import com.mongodb.operation.DropCollectionOperation;
import com.mongodb.operation.DropDatabaseOperation;
import com.mongodb.operation.Index;
import com.mongodb.operation.InsertOperation;
import com.mongodb.operation.InsertRequest;
import com.mongodb.operation.QueryOperation;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.codecs.Codec;
import org.mongodb.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ClusterFixture.getBinding;
import static java.util.Arrays.asList;

public final class CollectionHelper<T> {

    private Codec<T> codec;
    private MongoNamespace namespace;

    public static void drop(final MongoNamespace namespace) {
        new DropCollectionOperation(namespace).execute(getBinding());
    }

    public static void dropDatabase(final String name) {
        if (name == null) {
            return;
        }
        try {
            new DropDatabaseOperation(name).execute(getBinding());
        } catch (CommandFailureException e) {
            if (!e.getErrorMessage().contains("ns not found")) {
                throw e;
            }
        }
    }

    public void create(final CreateCollectionOptions options) {
        drop(namespace);
        new CreateCollectionOperation(namespace.getDatabaseName(), options).execute(getBinding());
    }

    public CollectionHelper(final Codec<T> codec, final MongoNamespace namespace) {
        this.codec = codec;
        this.namespace = namespace;
    }

    @SuppressWarnings("unchecked")
    public void insertDocuments(final T... documents) {
        for (T document : documents) {
            new InsertOperation<T>(namespace, true, WriteConcern.ACKNOWLEDGED,
                                   asList(new InsertRequest<T>(document)), codec).execute(getBinding());
        }
    }

    @SuppressWarnings("unchecked")
    public void insertDocuments(final List<T> documents) {
        for (T document : documents) {
            new InsertOperation<T>(namespace, true, WriteConcern.ACKNOWLEDGED,
                                   asList(new InsertRequest<T>(document)), codec).execute(getBinding());
        }
    }

    @SuppressWarnings("unchecked")
    public <I> void insertDocuments(final Codec<I> iCodec, final I... documents) {
        for (I document : documents) {
            new InsertOperation<I>(namespace, true, WriteConcern.ACKNOWLEDGED,
                                   asList(new InsertRequest<I>(document)), iCodec).execute(getBinding());
        }
    }

    public List<T> find() {
        MongoCursor<T> cursor = new QueryOperation<T>(namespace, codec).execute(getBinding());
        List<T> results = new ArrayList<T>();
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }
        return results;
    }

    public List<T> find(final Document filter) {
        QueryOperation<T> queryOperation = new QueryOperation<T>(namespace, codec);
        queryOperation.setCriteria(wrap(filter));
        MongoCursor<T> cursor = queryOperation.execute(getBinding());
        List<T> results = new ArrayList<T>();
        while (cursor.hasNext()) {
            results.add(cursor.next());
        }
        return results;
    }

    public long count() {
        return new CountOperation(namespace).execute(getBinding());
    }

    public long count(final Document criteria) {
        CountOperation operation = new CountOperation(namespace);
        operation.setCriteria(wrap(criteria));
        return operation.execute(getBinding());
    }

    public BsonDocument wrap(final Document document) {
        return new BsonDocumentWrapper<Document>(document, new DocumentCodec());
    }

    public void createIndexes(final List<Index> indexes) {
        new CreateIndexesOperation(namespace, indexes).execute(getBinding());
    }
}
