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
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding

class FindAndDeleteOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()

    def 'should remove single document'() {

        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam)

        when:
        FindAndDeleteOperation<Document> operation = new FindAndDeleteOperation<Document>(getNamespace(), documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = operation.execute(getBinding())

        then:
        getCollectionHelper().find().size() == 1;
        getCollectionHelper().find().first().getString('name') == 'Sam'
        returnedDocument.getString('name') == 'Pete'
    }


    @Category(Async)
    def 'should remove single document asynchronously'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam)

        when:
        FindAndDeleteOperation<Document> operation = new FindAndDeleteOperation<Document>(getNamespace(), documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = executeAsync(operation)

        then:
        getCollectionHelper().find().size() == 1;
        getCollectionHelper().find().first().getString('name') == 'Sam'
        returnedDocument.getString('name') == 'Pete'
    }

    def 'should remove single document when using custom codecs'() {
        given:
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 7)
        getWorkerCollectionHelper().insertDocuments(new WorkerCodec(), pete, sam)

        when:
        FindAndDeleteOperation<Worker> operation = new FindAndDeleteOperation<Document>(getNamespace(), workerCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = operation.execute(getBinding())

        then:
        getWorkerCollectionHelper().find().size() == 1;
        getWorkerCollectionHelper().find().first() == sam
        returnedDocument == pete
    }

    @Category(Async)
    def 'should remove single document when using custom codecs asynchronously'() {
        given:
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 7)
        getWorkerCollectionHelper().insertDocuments(new WorkerCodec(), pete, sam)

        when:
        FindAndDeleteOperation<Worker> operation = new FindAndDeleteOperation<Document>(getNamespace(), workerCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = operation.execute(getBinding())

        then:
        getWorkerCollectionHelper().find().size() == 1;
        getWorkerCollectionHelper().find().first() == sam
        returnedDocument == pete
    }

}
