/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.operation

import category.Slow
import com.mongodb.DuplicateKeyException
import com.mongodb.MongoClientException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.bulk.InsertRequest
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonSerializationException
import org.bson.codecs.BsonDocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getAsyncSingleConnectionBinding
import static com.mongodb.ClusterFixture.getSingleConnectionBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static java.util.Arrays.asList

class InsertOperationSpecification extends OperationFunctionalSpecification {

    def 'should throw IllegalArgumentException for empty list of requests'() {
        when:
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, true, [])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should return correct result'() {
        given:
        def inserts = [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                       new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))]
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, inserts)

        when:
        def result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()

        inserts*.getDocument() == getCollectionHelper().find(new BsonDocumentCodec())

        where:
        async << [true, false]
    }

    def 'should insert a single document'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, asList(insert))

        when:
        execute(operation, async)

        then:
        asList(insert.getDocument()) == getCollectionHelper().find(new BsonDocumentCodec())

        where:
        async << [true, false]
    }

    def 'should insert a large number of documents'() {
        given:
        def inserts = []
        for (i in 1..1001) {
            inserts += new InsertRequest(new BsonDocument('_id', new BsonInt32(i)))
        }
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, inserts.toList())


        when:
        execute(operation, async)

        then:
        getCollectionHelper().count() == 1001

        where:
        async << [true, false]
    }

    def 'should execute unacknowledged write'() {
        given:
        def binding = async ? getAsyncSingleConnectionBinding() : getSingleConnectionBinding()
        def operation = new InsertOperation(getNamespace(), true, UNACKNOWLEDGED, false,
                [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                 new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))])

        when:
        def result = execute(operation, binding)

        then:
        !result.wasAcknowledged()
        getCollectionHelper().count(binding) == 2

        cleanup:
        binding?.release()

        where:
        async << [true, false]
    }

    @Category(Slow)
    def 'should insert a batch at The limit of the batch size'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 2127]
        byte[] smallerByteArray = new byte[1024 * 16 + 1980]
        def documents = [
                new InsertRequest(new BsonDocument('bytes', new BsonBinary(hugeByteArray))),
                new InsertRequest(new BsonDocument('bytes', new BsonBinary(smallerByteArray)))
        ]
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, documents.toList())

        when:
        execute(operation, async)

        then:
        getCollectionHelper().count() == 2

        where:
        async << [true, false]
    }

    def 'should continue on error when continuing on error'() {
        given:
        def documents = [
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(2))),
        ]
        def operation = new InsertOperation(getNamespace(), false, ACKNOWLEDGED, false, documents)

        when:
        execute(operation, async)

        then:
        thrown(DuplicateKeyException)
        getCollectionHelper().count() == 2

        where:
        async << [true, false]
    }

    def 'should not continue on error when not continuing on error'() {
        given:
        def documents = [
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(2))),
        ]
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, documents)

        when:
        execute(operation, async)

        then:
        thrown(DuplicateKeyException)
        getCollectionHelper().count() == 1

        where:
        async << [true, false]
    }

    def 'should throw exception if document is too large'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16]
        def documents = [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)).append('b', new BsonBinary(hugeByteArray)))]
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, documents)

        when:
        execute(operation, async)

        then:
        thrown(BsonSerializationException)

        where:
        async << [true, false]
    }

    def 'should move _id to the beginning'() {
        given:
        def insert = new InsertRequest(new BsonDocument('x', new BsonInt32(1)).append('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, false, asList(insert))

        when:
        execute(operation, async)

        then:
        getCollectionHelper().find().get(0).keySet() as List == ['_id', 'x']

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
    def 'should throw if bypassDocumentValidation is set and write is unacknowledged'() {
        given:
        def operation = new InsertOperation(getNamespace(), true,  UNACKNOWLEDGED, false, [new InsertRequest(new BsonDocument())])
                .bypassDocumentValidation(bypassDocumentValidation)

        when:
        execute(operation, async)

        then:
        thrown(MongoClientException)

        where:
        [async, bypassDocumentValidation] << [[true, false], [true, false]].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) || !isDiscoverableReplicaSet() })
    def 'should support retryable writes'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = new InsertOperation(getNamespace(), true, ACKNOWLEDGED, true, asList(insert))

        when:
        executeWithSession(operation, async)

        then:
        asList(insert.getDocument()) == getCollectionHelper().find(new BsonDocumentCodec())

        where:
        async << [true, false]
    }
}
