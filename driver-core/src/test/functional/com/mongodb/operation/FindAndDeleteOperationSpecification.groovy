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

import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.client.test.CollectionHelper
import com.mongodb.client.test.Worker
import com.mongodb.client.test.WorkerCodec
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList

class FindAndDeleteOperationSpecification extends OperationFunctionalSpecification {
    private final DocumentCodec documentCodec = new DocumentCodec()
    private final WorkerCodec workerCodec = new WorkerCodec()
    def writeConcern = WriteConcern.ACKNOWLEDGED

    def 'should have the correct defaults'() {
        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)

        then:
        operation.getNamespace() == getNamespace()
        operation.getWriteConcern() == writeConcern
        operation.getDecoder() == documentCodec
        operation.getFilter() == null
        operation.getSort() == null
        operation.getProjection() == null
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 0
        operation.getCollation() == null
    }

    def 'should set optional values correctly'(){
        given:
        def filter = BsonDocument.parse('{ filter : 1}')
        def sort = BsonDocument.parse('{ sort : 1}')
        def projection = BsonDocument.parse('{ projection : 1}')

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
            .filter(filter)
            .sort(sort)
            .projection(projection)
            .maxTime(10, TimeUnit.MILLISECONDS)
            .collation(defaultCollation)

        then:
        operation.getFilter() == filter
        operation.getSort() == sort
        operation.getProjection() == projection
        operation.getMaxTime(TimeUnit.MILLISECONDS) == 10
        operation.getCollation() == defaultCollation
    }

    def 'should remove single document'() {

        given:
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        Document sam = new Document('name', 'Sam').append('job', 'plumber')

        getCollectionHelper().insertDocuments(new DocumentCodec(), pete, sam)

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Document returnedDocument = execute(operation, async)

        then:
        getCollectionHelper().find().size() == 1;
        getCollectionHelper().find().first().getString('name') == 'Sam'
        returnedDocument.getString('name') == 'Pete'

        where:
        async << [true, false]
    }


    def 'should remove single document when using custom codecs'() {
        given:
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Worker sam = new Worker('Sam', 'plumber', new Date(), 7)
        getWorkerCollectionHelper().insertDocuments(new WorkerCodec(), pete, sam)

        when:
        FindAndDeleteOperation<Worker> operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, workerCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))
        Worker returnedDocument = execute(operation, async)

        then:
        getWorkerCollectionHelper().find().size() == 1;
        getWorkerCollectionHelper().find().first() == sam
        returnedDocument == pete

        where:
        async << [true, false]
    }


    @IgnoreIf({ !serverVersionAtLeast(asList(3, 2, 0)) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        CollectionHelper<Document> helper = new CollectionHelper<Document>(documentCodec, getNamespace())
        Document pete = new Document('name', 'Pete').append('job', 'handyman')
        helper.insertDocuments(new DocumentCodec(), pete)

        when:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), new WriteConcern(5, 1), documentCodec)
                .filter(new BsonDocument('name', new BsonString('Pete')))

        then:
        testOperationThrows(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        !ex.writeConcernError.message.isEmpty()
        ex.writeResult.count == 1
        !ex.writeResult.updateOfExisting
        ex.writeResult.upsertedId == null

        where:
        async << [true, false]
    }

    def 'should create the expected command'() {
        when:
        def cannedResult = new BsonDocument('value', new BsonDocumentWrapper(BsonDocument.parse('{}'), new BsonDocumentCodec()))
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
        def expectedCommand = new BsonDocument('findandmodify', new BsonString(getNamespace().getCollectionName()))
                .append('remove', BsonBoolean.TRUE)

        if (includeWriteConcern) {
            expectedCommand.put('writeConcern', writeConcern.asDocument())
        }

        then:
        testOperation(operation, serverVersion, expectedCommand, async, cannedResult)

        when:
        def filter = BsonDocument.parse('{ filter : 1}')
        def sort = BsonDocument.parse('{ sort : 1}')
        def projection = BsonDocument.parse('{ projection : 1}')

        operation.filter(filter)
                .sort(sort)
                .projection(projection)
                .maxTime(10, TimeUnit.MILLISECONDS)

        expectedCommand.append('query', filter)
                .append('sort', sort)
                .append('fields', projection)
                .append('maxTimeMS', new BsonInt64(10))

        if (includeCollation) {
            operation.collation(defaultCollation)
            expectedCommand.append('collation', defaultCollation.asDocument())
        }

        then:
        testOperation(operation, serverVersion, expectedCommand, async, cannedResult)

        where:
        serverVersion | writeConcern                 | includeWriteConcern | includeCollation | async
        [3, 4, 0]     | WriteConcern.W1              | true                | true             | true
        [3, 4, 0]     | WriteConcern.ACKNOWLEDGED    | false               | true             | true
        [3, 4, 0]     | WriteConcern.UNACKNOWLEDGED  | false               | true             | true
        [3, 4, 0]     | WriteConcern.W1              | true                | true             | false
        [3, 4, 0]     | WriteConcern.ACKNOWLEDGED    | false               | true             | false
        [3, 4, 0]     | WriteConcern.UNACKNOWLEDGED  | false               | true             | false
        [3, 0, 0]     | WriteConcern.ACKNOWLEDGED    | false               | false            | true
        [3, 0, 0]     | WriteConcern.UNACKNOWLEDGED  | false               | false            | true
        [3, 0, 0]     | WriteConcern.W1              | false               | false            | true
        [3, 0, 0]     | WriteConcern.ACKNOWLEDGED    | false               | false            | false
        [3, 0, 0]     | WriteConcern.UNACKNOWLEDGED  | false               | false            | false
        [3, 0, 0]     | WriteConcern.W1              | false               | false            | false
    }

    def 'should throw an exception when passing an unsupported collation'() {
        given:
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec).collation(defaultCollation)

        when:
        testOperationThrows(operation, [3, 2, 0], async)

        then:
        def exception = thrown(IllegalArgumentException)
        exception.getMessage().startsWith('Collation not supported by server version:')

        where:
        async << [false, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 3, 10)) })
    def 'should support collation'() {
        given:
        def document = Document.parse('{_id: 1, str: "foo"}')
        getCollectionHelper().insertDocuments(document)
        def operation = new FindAndDeleteOperation<Document>(getNamespace(), writeConcern, documentCodec)
                .filter(BsonDocument.parse('{str: "FOO"}'))
                .collation(caseInsensitiveCollation)

        when:
        def result = execute(operation, async)

        then:
        result == document

        where:
        async << [true, false]
    }
}
