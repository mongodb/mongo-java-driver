/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.operation

import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoCollection
import org.mongodb.codecs.DocumentCodec
import org.mongodb.test.Worker
import org.mongodb.test.WorkerCodec

import static org.mongodb.Fixture.bufferProvider
import static org.mongodb.Fixture.session

class FindAndRemoveOperationSpecification extends FunctionalSpecification {
    private final DocumentCodec documentDecoder = new DocumentCodec()

    def 'should remove single document'() {
        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        collection.insert(pete);
        collection.insert(sam);

        when:
        FindAndRemove findAndRemove = new FindAndRemove().where(new Document('name', 'Pete'));

        FindAndRemoveOperation<Document> operation = new FindAndRemoveOperation<Document>(collection.namespace, findAndRemove,
                                                                                          documentDecoder, bufferProvider,
                                                                                          session, false)
        Document returnedDocument = operation.execute()

        then:
        collection.find().count() == 1;
        collection.find().one == sam
        returnedDocument == pete
    }

    def 'should remove single document when using custom codecs'() {
        given:
        MongoCollection<Worker> workerCollection = database.getCollection(getCollectionName(), new WorkerCodec())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 7)

        workerCollection.insert(pete);
        workerCollection.insert(sam);

        when:
        FindAndRemove<Worker> findAndRemove = new FindAndRemove<Worker>().where(new Document('name', 'Pete'));

        FindAndRemoveOperation<Worker> operation = new FindAndRemoveOperation<Worker>(collection.namespace, findAndRemove,
                                                                                      new WorkerCodec(), bufferProvider, session,
                                                                                      false)
        Worker returnedDocument = operation.execute()

        then:
        workerCollection.find().count() == 1;
        workerCollection.find().one == sam
        returnedDocument == pete
    }
}
