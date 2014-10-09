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

package com.mongodb

import com.mongodb.binding.AsyncSingleConnectionBinding
import com.mongodb.binding.PinnedBinding
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import com.mongodb.connection.ServerHelper
import com.mongodb.operation.InsertOperation
import com.mongodb.operation.InsertRequest
import org.bson.BsonDocument
import org.bson.codecs.DocumentCodec
import org.bson.types.Document
import spock.lang.Specification

import static com.mongodb.ClusterFixture.getPrimary
import static com.mongodb.WriteConcern.ACKNOWLEDGED

class OperationFunctionalSpecification extends Specification {

    def setupSpec() {
        Runtime.getRuntime().addShutdownHook { CollectionHelper.dropDatabase(getDatabaseName()) }
        CollectionHelper.dropDatabase(getDatabaseName())
    }

    def cleanupSpec() {
        CollectionHelper.dropDatabase(getDatabaseName())
    }

    def setup() {
        CollectionHelper.drop(getNamespace())
    }

    def cleanup() {
        CollectionHelper.drop(getNamespace())
        ServerHelper.checkPool(getPrimary())
    }

    String getDatabaseName() {
        ClusterFixture.getDefaultDatabaseName();
    }

    String getCollectionName() {
        getClass().getName();
    }

    MongoNamespace getNamespace() {
        new MongoNamespace(getDatabaseName(), getCollectionName())
    }

    void acknowledgeWrite(PinnedBinding binding) {
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                      [new InsertRequest(new BsonDocument())]).execute(binding);
        binding.release();
    }

    void acknowledgeWrite(AsyncSingleConnectionBinding binding) {
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                      [new InsertRequest(new BsonDocument())]).executeAsync(binding).get();
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
