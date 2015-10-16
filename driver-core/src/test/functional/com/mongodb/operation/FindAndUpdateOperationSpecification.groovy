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

package com.mongodb.operation

import category.Async
import com.mongodb.MongoCommandException
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.client.model.Filters.gte
import static java.util.Arrays.asList

class FindAndUpdateOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()

    def 'should have the correct defaults and passed values'() {
        when:
        def update = new BsonDocument('update', new BsonInt32(1))
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update)

        then:
        operation.getNamespace() == getNamespace()
        operation.getDecoder() == documentCodec
        operation.getUpdate() == update
        operation.getFilter() == null
        operation.getSort() == null
        operation.getProjection() == null
        operation.getMaxTime(TimeUnit.SECONDS) == 0
        operation.getBypassDocumentValidation() == null
    }

    def 'should set optional values correctly'(){
        given:
        def filter = new BsonDocument('filter', new BsonInt32(1))
        def sort = new BsonDocument('sort', new BsonInt32(1))
        def projection = new BsonDocument('projection', new BsonInt32(1))

        when:
        def operation = new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, new BsonDocument('update', new BsonInt32(1)))
                .filter(filter).sort(sort).projection(projection).bypassDocumentValidation(true).maxTime(1, TimeUnit.SECONDS).upsert(true)
                .returnOriginal(false)

        then:
        operation.getFilter() == filter
        operation.getSort() == sort
        operation.getProjection() == projection
        operation.upsert == true
        operation.getMaxTime(TimeUnit.SECONDS) == 1
        operation.getBypassDocumentValidation()
        !operation.isReturnOriginal()
    }

    def 'should update single document'() {

        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('numberOfJobs', 3)
        Document sam = new Document('name', 'Sam').append('numberOfJobs', 5)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        FindAndUpdateOperation<Document> operation = new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.getInteger('numberOfJobs') == 3
        helper.find().size() == 2;
        helper.find().get(0).getInteger('numberOfJobs') == 4

        when:
        update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        operation = new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.getInteger('numberOfJobs') == 5
    }

    @Category(Async)
    def 'should update single document asynchronously'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('numberOfJobs', 3)
        Document sam = new Document('name', 'Sam').append('numberOfJobs', 5)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        FindAndUpdateOperation<Document> operation = new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(true)
        Document returnedDocument = executeAsync(operation)

        then:
        returnedDocument.getInteger('numberOfJobs') == 3
        helper.find().size() == 2;
        helper.find().get(0).getInteger('numberOfJobs') == 4

        when:
        update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        operation = new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        returnedDocument = executeAsync(operation)

        then:
        returnedDocument.getInteger('numberOfJobs') == 5
    }

    def 'should update single document when using custom codecs'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)

        helper.insertDocuments(new WorkerCodec(), pete, sam)

        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        FindAndUpdateOperation<Worker> operation = new FindAndUpdateOperation<Worker>(getNamespace(), workerCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.numberOfJobs == 3
        helper.find().size() == 2;
        helper.find().get(0).numberOfJobs == 4

        when:
        update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        operation = new FindAndUpdateOperation<Worker>(getNamespace(), workerCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.numberOfJobs == 5
    }

    @Category(Async)
    def 'should update single document when using custom codecs asynchronously'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)

        helper.insertDocuments(new WorkerCodec(), pete, sam)

        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        FindAndUpdateOperation<Worker> operation = new FindAndUpdateOperation<Worker>(getNamespace(), workerCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = executeAsync(operation)

        then:
        returnedDocument.numberOfJobs == 3
        helper.find().size() == 2;
        helper.find().get(0).numberOfJobs == 4

        when:
        update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        operation = new FindAndUpdateOperation<Worker>(getNamespace(), workerCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
                .returnOriginal(false)
        returnedDocument = executeAsync(operation)

        then:
        returnedDocument.numberOfJobs == 5
    }

    def 'should return null if query fails to match'() {
        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        FindAndUpdateOperation<Document> operation = new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument == null
    }

    @Category(Async)
    def 'should return null if query fails to match asynchronously'() {
        when:
        def update = new BsonDocument('$inc', new BsonDocument('numberOfJobs', new BsonInt32(1)))
        FindAndUpdateOperation<Document> operation = new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = executeAsync(operation)

        then:
        returnedDocument == null
    }

    def 'should throw an exception if update contains fields that are not update operators'() {
        when:
        def update = new BsonDocument('x', new BsonInt32(1))
        new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update).execute(getBinding())

        then:
        thrown(IllegalArgumentException)
    }

    @Category(Async)
    def 'should throw an exception if update contains fields that are not update operators asynchronously'() {
        when:
        def update = new BsonDocument('x', new BsonInt32(1))
        executeAsync(new FindAndUpdateOperation<Document>(getNamespace(), documentCodec, update))

        then:
        def ex = thrown(MongoException)
        ex.getCause() instanceof IllegalArgumentException
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collectionOut')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        collectionHelper.insertDocuments(BsonDocument.parse('{ level: 10 }'))

        when:
        def update = new BsonDocument('$inc', new BsonDocument('level', new BsonInt32(-1)))
        FindAndUpdateOperation<Document> operation = new FindAndUpdateOperation<Document>(namespace, documentCodec, update)
        operation.execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        operation.bypassDocumentValidation(false).execute(getBinding())

        then:
        thrown(MongoCommandException)

        when:
        Document returnedDocument = operation.bypassDocumentValidation(true).returnOriginal(false).execute(getBinding())

        then:
        notThrown(MongoCommandException)
        returnedDocument.getInteger('level') == 9

        cleanup:
        collectionHelper?.drop()
    }

    @Category(Async)
    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should support bypassDocumentValidation asynchronously'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collectionOut')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create('collectionOut', new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        collectionHelper.insertDocuments(BsonDocument.parse('{ level: 10 }'))

        when:
        def update = new BsonDocument('$inc', new BsonDocument('level', new BsonInt32(-1)))
        FindAndUpdateOperation<Document> operation = new FindAndUpdateOperation<Document>(namespace, documentCodec, update)
        executeAsync(operation)

        then:
        thrown(MongoCommandException)

        when:
        executeAsync(operation.bypassDocumentValidation(false))

        then:
        thrown(MongoCommandException)

        when:
        Document returnedDocument = executeAsync(operation.bypassDocumentValidation(true).returnOriginal(false))

        then:
        notThrown(MongoCommandException)
        returnedDocument.getInteger('level') == 9

        cleanup:
        collectionHelper?.drop()
    }

}
