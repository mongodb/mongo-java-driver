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
import com.mongodb.MongoException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding

class FindAndReplaceOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()

    def 'should replace single document'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(), documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.getString('name') == 'Pete'
        helper.find().size() == 2;
        helper.find().get(0).getString('name') == 'Jordan'

        when:
        operation = new FindAndReplaceOperation<Document>(getNamespace(), documentCodec,
                                                          new BsonDocumentWrapper<Document>(pete, documentCodec))
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.getString('name') == 'Pete'
    }

    @Category(Async)
    def 'should replace single document asynchronously'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)

        helper.insertDocuments(new DocumentCodec(), pete, sam)

        when:
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(), documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = executeAsync(operation)

        then:
        returnedDocument.getString('name') == 'Pete'
        helper.find().size() == 2;
        helper.find().get(0).getString('name') == 'Jordan'

        when:
        operation = new FindAndReplaceOperation<Document>(getNamespace(), documentCodec,
                                                          new BsonDocumentWrapper<Document>(pete, documentCodec))
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = executeAsync(operation)

        then:
        returnedDocument.getString('name') == 'Pete'
    }

    def 'should replace single document when using custom codecs'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)
        Worker jordan = new Worker(pete.id, 'Jordan', 'sparky', new Date(), 7)
        BsonDocument replacement = new BsonDocumentWrapper<Worker>(jordan, workerCodec)

        helper.insertDocuments(new WorkerCodec(), pete, sam)

        when:
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(), workerCodec, replacement)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument == pete
        helper.find().get(0) == jordan

        when:
        replacement = new BsonDocumentWrapper<Worker>(pete, workerCodec)
        operation = new FindAndReplaceOperation<Document>(getNamespace(), workerCodec, replacement)
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument == pete
    }

    @Category(Async)
    def 'should replace single document when using custom codecs asynchronously'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)
        Worker jordan = new Worker(pete.id, 'Jordan', 'sparky', new Date(), 7)
        BsonDocument replacement = new BsonDocumentWrapper<Worker>(jordan, workerCodec)

        helper.insertDocuments(new WorkerCodec(), pete, sam)

        when:
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(), workerCodec, replacement)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = executeAsync(operation)

        then:
        returnedDocument == pete
        helper.find().get(0) == jordan

        when:
        replacement = new BsonDocumentWrapper<Worker>(pete, workerCodec)
        operation = new FindAndReplaceOperation<Document>(getNamespace(), workerCodec, replacement)
                .filter(new BsonDocument('name', new BsonString('Jordan')))
                .returnOriginal(false)
        returnedDocument = executeAsync(operation)

        then:
        returnedDocument == pete
    }

    def 'should return null if query fails to match'() {
        when:
        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(), documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument == null
    }

    @Category(Async)
    def 'should return null if query fails to match asynchronously'() {
        when:
        BsonDocument jordan = new BsonDocumentWrapper<Document>([name: 'Jordan', job: 'sparky'] as Document, documentCodec)
        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(), documentCodec, jordan)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = executeAsync(operation)

        then:
        returnedDocument == null
    }

    def 'should throw an exception if replacement contains update operators'() {
        when:
        def replacement = new BsonDocumentWrapper<Document>(['$inc': 1] as Document, documentCodec)
        new FindAndReplaceOperation<Document>(getNamespace(), documentCodec, replacement).execute(getBinding())

        then:
        thrown(IllegalArgumentException)
    }

    @Category(Async)
    def 'should throw an exception if replacement contains update operators asynchronously'() {
        when:
        def replacement = new BsonDocumentWrapper<Document>(['$inc': 1] as Document, documentCodec)
        executeAsync(new FindAndReplaceOperation<Document>(getNamespace(), documentCodec, replacement))

        then:
        def ex = thrown(MongoException)
        ex.getCause() instanceof IllegalArgumentException
    }
}
