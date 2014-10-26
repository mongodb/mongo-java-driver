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
import com.mongodb.MongoException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.bulk.InsertRequest
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonSerializationException
import org.bson.codecs.BsonDocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getAsyncSingleConnectionBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getPinnedBinding
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
        def result = operation.executeAsync(getAsyncBinding()).get()

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
        operation.executeAsync(getAsyncBinding()).get()

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
        operation.executeAsync(getAsyncBinding()).get()

        then:
        getCollectionHelper().count() == 1001
    }

    def 'should return UnacknowledgedWriteResult when using an unacknowledged WriteConcern'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, UNACKNOWLEDGED, asList(insert))

        when:
        def binding = getPinnedBinding()
        def result = operation.execute(binding)
        new InsertOperation(namespace, true, ACKNOWLEDGED,
                            [new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))]).execute(binding);

        then:
        !result.wasAcknowledged()

        cleanup:
        acknowledgeWrite(binding)
    }

    @Category(Async)
    def 'should return UnacknowledgedWriteResult when using an unacknowledged WriteConcern asynchronously'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, UNACKNOWLEDGED, asList(insert))

        when:
        def binding = getAsyncSingleConnectionBinding()
        def result = operation.executeAsync(binding).get()

        then:
        !result.wasAcknowledged()

        cleanup:
        acknowledgeWrite(binding)
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
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents.toList()).executeAsync(getAsyncBinding()).get()

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
        thrown(MongoException.DuplicateKey)
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
        new InsertOperation(getNamespace(), false, ACKNOWLEDGED, documents).executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoException.DuplicateKey)
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
        thrown(MongoException.DuplicateKey)
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
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents).executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoException.DuplicateKey)
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
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, documents).executeAsync(getAsyncBinding()).get()

        then:
        thrown(BsonSerializationException)
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
}
