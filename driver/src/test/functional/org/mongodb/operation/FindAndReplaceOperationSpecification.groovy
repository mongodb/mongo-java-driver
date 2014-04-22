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

package org.mongodb.operation
import category.Async
import org.junit.experimental.categories.Category
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.codecs.DocumentCodec
import org.mongodb.test.CollectionHelper
import org.mongodb.test.Worker
import org.mongodb.test.WorkerCodec

import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding

//TODO: what about custom Date formats?
//TODO: test null returns
class FindAndReplaceOperationSpecification extends FunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()

    def 'should replace single document'() {

        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document jordan = new Document('name', 'Jordan').append('job', 'sparky')

        helper.insertDocuments(pete, sam)

        when:
        FindAndReplace<Document> findAndReplace = new FindAndReplace<Document>(jordan).where(new Document('name', 'Pete'));

        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(), findAndReplace,
                documentCodec, documentCodec)
        Document returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument.getString('name') == 'Pete'
        helper.find().size() == 2;
        helper.find().get(0).getString('name') == 'Jordan'
    }

    @Category(Async)
    def 'should replace single document asynchronously'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')
        Document jordan = new Document('name', 'Jordan').append('job', 'sparky')

        helper.insertDocuments(pete, sam)

        when:
        FindAndReplace<Document> findAndReplace = new FindAndReplace<Document>(jordan).where(new Document('name', 'Pete'));

        FindAndReplaceOperation<Document> operation = new FindAndReplaceOperation<Document>(getNamespace(), findAndReplace,
                documentCodec, documentCodec)
        Document returnedDocument = operation.executeAsync(getAsyncBinding()).get()

        then:
        returnedDocument.getString('name') == 'Pete'
        helper.find().size() == 2;
        helper.find().get(0).getString('name') == 'Jordan'
    }

    def 'should replace single document when using custom codecs'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)
        Worker jordan = new Worker(pete.id, 'Jordan', 'sparky', new Date(), 7)

        helper.insertDocuments(pete, sam)

        when:
        FindAndReplace<Worker> findAndReplace = new FindAndReplace<Worker>(jordan).where(new Document('name', 'Pete'));

        FindAndReplaceOperation<Worker> operation = new FindAndReplaceOperation<Worker>(getNamespace(), findAndReplace,
                workerCodec, workerCodec)
        Worker returnedDocument = operation.execute(getBinding())

        then:
        returnedDocument == pete
        helper.find().get(0) == jordan
    }

    @Category(Async)
    def 'should replace single document when using custom codecs asynchronously'() {
        given:
        CollectionHelper<Worker> helper = new CollectionHelper<Worker>(workerCodec, getNamespace())
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 5)
        Worker jordan = new Worker(pete.id, 'Jordan', 'sparky', new Date(), 7)

        helper.insertDocuments(pete, sam)

        when:
        FindAndReplace<Worker> findAndReplace = new FindAndReplace<Worker>(jordan).where(new Document('name', 'Pete'));

        FindAndReplaceOperation<Worker> operation = new FindAndReplaceOperation<Worker>(getNamespace(), findAndReplace,
                workerCodec, workerCodec)
        Worker returnedDocument = operation.executeAsync(getAsyncBinding()).get()

        then:
        returnedDocument == pete
        helper.find().get(0) == jordan
    }
}
