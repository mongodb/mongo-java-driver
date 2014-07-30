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

package com.mongodb.client

import com.mongodb.MongoNamespace
import com.mongodb.binding.AsyncSingleConnectionBinding
import com.mongodb.binding.PinnedBinding
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import com.mongodb.codecs.DocumentCodec
import com.mongodb.connection.ServerHelper
import com.mongodb.operation.InsertOperation
import com.mongodb.operation.InsertRequest
import org.mongodb.Document
import spock.lang.Specification

import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.Fixture.getDefaultDatabaseName
import static com.mongodb.Fixture.getMongoClient
import static com.mongodb.WriteConcern.ACKNOWLEDGED

class DatabaseSpecification extends Specification {
    protected MongoDatabase database;
    protected MongoCollection<Document> collection;

    def setup() {
        database = getMongoClient().getDatabase(getDefaultDatabaseName())
        collection = database.getCollection(getClass().getName())
        collection.tools().drop()
    }

    def cleanup() {
        if (collection != null) {
            collection.tools().drop();
        }
        ServerHelper.checkPool(getPrimary())
    }

    String getDatabaseName() {
        database.getName();
    }

    String getCollectionName() {
        collection.getName();
    }

    MongoNamespace getNamespace() {
        new MongoNamespace(getDatabaseName(), getCollectionName())
    }


    void acknowledgeWrite(PinnedBinding binding) {
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                      [new InsertRequest<Document>(new Document())], new DocumentCodec()).execute(binding);
        binding.release();
    }

    void acknowledgeWrite(AsyncSingleConnectionBinding binding) {
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                      [new InsertRequest<Document>(new Document())], new DocumentCodec()).executeAsync(binding).get();
        binding.release();
    }

    CollectionHelper<Document> getCollectionHelper() {
        getCollectionHelper(getNamespace())
    }

    CollectionHelper<Document> getCollectionHelper(MongoNamespace namespace) {
        new CollectionHelper<Document>(new DocumentCodec(), namespace)
    }

    CollectionHelper<Worker> getWorkerCollectionHelper() {
        new CollectionHelper<Worker>(new WorkerCodec(), getNamespace())
    }
}
