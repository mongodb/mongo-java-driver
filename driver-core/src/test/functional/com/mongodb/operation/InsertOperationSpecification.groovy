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
import category.Slow
import com.mongodb.DuplicateKeyException
import com.mongodb.MongoClientException
import com.mongodb.MongoException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.bulk.InsertRequest
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonSerializationException
import org.bson.codecs.BsonDocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getAsyncSingleConnectionBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getSingleConnectionBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static java.util.Arrays.asList

class InsertOperationSpecification extends OperationFunctionalSpecification {
    def 'should return correct result'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(insert))

        when:
        def result = operation.execute(getBinding())

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
    }

    @Category(Async)
    def 'should return correct result asynchronously'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(insert))

        when:
        def result = executeAsync(operation)

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
    }

    def 'should insert a single document'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(insert))

        when:
        operation.execute(getBinding())

        then:
        asList(insert.getDocument()) == getCollectionHelper().find(new BsonDocumentCodec())
    }

    @Category(Async)
    def 'should insert a single document asynchronously'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(insert))

        when:
        executeAsync(operation)

        then:
        asList(insert.getDocument()) == getCollectionHelper().find(new BsonDocumentCodec())
    }

    def 'should insert a large number of documents'() {
        given:
        def inserts = []
        for (i in 1..1001) {
            inserts += new InsertRequest(new BsonDocument('_id', new BsonInt32(i)))
        }
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, inserts.toList())


        when:
        operation.execute(getBinding())

        then:
        getCollectionHelper().count() == 1001
    }

    @Category(Async)
    def 'should insert a large number of documents asynchronously'() {
        given:
        def inserts = []
        for (i in 1..1001) {
            inserts += new InsertRequest(new BsonDocument('_id', new BsonInt32(i)))
        }
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, inserts.toList())


        when:
        executeAsync(operation)

        then:
        getCollectionHelper().count() == 1001
    }

    def 'should execute unacknowledged write'() {
        given:
        def binding = getSingleConnectionBinding()

        when:
        def result = new InsertOperation(getNamespace(), true, UNACKNOWLEDGED,
                                         [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))])
                .execute(binding)

        then:
        !result.wasAcknowledged()
        getCollectionHelper().count(binding) == 1

        cleanup:
        binding?.release()
    }

    @Category(Async)
    def 'should execute unacknowledged write asynchronously'() {
        given:
        def binding = getAsyncSingleConnectionBinding()

        when:
        def result = executeAsync(new InsertOperation(getNamespace(), true, UNACKNOWLEDGED,
                                                      [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))]), binding)

        then:
        !result.wasAcknowledged()
        getCollectionHelper().count(binding) == 1

        cleanup:
        binding?.release()
    }

    @Category(Slow)
    def 'should insert a batch at The limit of the batch size'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 2127];
        byte[] smallerByteArray = new byte[1024 * 16 + 1980];

        def documents = [
                new InsertRequest(new BsonDocument('bytes', new BsonBinary(hugeByteArray))),
                new InsertRequest(new BsonDocument('bytes', new BsonBinary(smallerByteArray)))
        ]

        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents.toList()).execute(getBinding())

        then:
        getCollectionHelper().count() == 2
    }

    @Category([Async, Slow])
    def 'should insert a batch at The limit of the batch size asynchronously'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 2127];
        byte[] smallerByteArray = new byte[1024 * 16 + 1980];

        def documents = [
                new InsertRequest(new BsonDocument('bytes', new BsonBinary(hugeByteArray))),
                new InsertRequest(new BsonDocument('bytes', new BsonBinary(smallerByteArray)))
        ]

        when:
        executeAsync(new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents.toList()))

        then:
        getCollectionHelper().count() == 2
    }

    def 'should continue on error when continuing on error'() {
        given:
        def documents = [
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(2))),
        ]

        when:
        new InsertOperation(getNamespace(), false, ACKNOWLEDGED, documents).execute(getBinding())

        then:
        thrown(DuplicateKeyException)
        getCollectionHelper().count() == 2
    }

    @Category(Async)
    def 'should continue on error when continuing on error asynchronously'() {
        given:
        def documents = [
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(2))),
        ]

        when:
        executeAsync(new InsertOperation(getNamespace(), false, ACKNOWLEDGED, documents))

        then:
        thrown(DuplicateKeyException)
        getCollectionHelper().count() == 2
    }

    def 'should not continue on error when not continuing on error'() {
        given:
        def documents = [
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(2))),
        ]

        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents).execute(getBinding())

        then:
        thrown(DuplicateKeyException)
        getCollectionHelper().count() == 1
    }

    @Category(Async)
    def 'should not continue on error when not continuing on error asynchronously'() {
        given:
        def documents = [
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(2))),
        ]

        when:
        executeAsync(new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents))

        then:
        thrown(DuplicateKeyException)
        getCollectionHelper().count() == 1
    }

    def 'should throw exception if document is too large'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16];
        def documents = [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)).append('b', new BsonBinary(hugeByteArray)))]

        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents).execute(getBinding())

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw exception if document is too large asynchronously'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16];
        def documents = [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)).append('b', new BsonBinary(hugeByteArray)))]

        when:
        executeAsync(new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents))

        then:
        def ex = thrown(MongoException)
        ex.getCause() instanceof BsonSerializationException
    }

    def 'should move _id to the beginning'() {
        given:
        def insert = new InsertRequest(new BsonDocument('x', new BsonInt32(1)).append('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(insert))

        when:
        operation.execute(getBinding())

        then:
        getCollectionHelper().find().get(0).keySet() as List == ['_id', 'x']
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
    def 'should throw if bypassDocumentValidation is set and write is unacknowledged'() {
        given:
        def op = new InsertOperation(getNamespace(), true,  UNACKNOWLEDGED, [new InsertRequest(new BsonDocument())])
                .bypassDocumentValidation(bypassDocumentValidation)

        when:
        op.execute(getBinding())

        then:
        thrown(MongoClientException)

        when:
        executeAsync(op)

        then:
        thrown(MongoClientException)

        where:
        bypassDocumentValidation << [true, false]
    }
}
