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
import com.mongodb.bulk.UpdateRequest
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonSerializationException
import org.bson.codecs.BsonDocumentCodec
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.bulk.WriteRequest.Type.REPLACE
import static java.util.Arrays.asList

class UpdateOperationForReplacementSpecification extends OperationFunctionalSpecification {
    def 'should return correct result'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument(), new BsonDocument('_id', new BsonInt32(1)), REPLACE)
        def op = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, asList(replacement))

        when:
        def result = op.execute(getBinding())

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
    }

    @Category(Async)
    def 'should return correct result asynchronously'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument(), new BsonDocument('_id', new BsonInt32(1)), REPLACE)
        def op = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, asList(replacement))

        when:
        def result = executeAsync(op)

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
    }

    def 'should replace a single document'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(insert)).execute(getBinding())

        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
        def op = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, asList(replacement))

        when:
        def result = op.execute(getBinding())

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == null
        result.isUpdateOfExisting()
        asList(replacement.getUpdate()) == getCollectionHelper().find(new BsonDocumentCodec())
        getCollectionHelper().find().get(0).keySet().iterator().next() == '_id'
    }

    @Category(Async)
    def 'should replace a single document asynchronously'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(insert)).execute(getBinding())

        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
        def op = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, asList(replacement))


        when:
        def result = executeAsync(op)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == null
        result.isUpdateOfExisting()
        asList(replacement.getUpdate()) == getCollectionHelper().find(new BsonDocumentCodec())
    }

    def 'should upsert a single document'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
                .upsert(true)
        def op = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, asList(replacement))

        when:
        op.execute(getBinding())

        then:
        asList(replacement.getUpdate()) == getCollectionHelper().find(new BsonDocumentCodec())
    }

    def 'should upsert a single document asynchronously'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
                .upsert(true)
        def op = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, asList(replacement))

        when:
        executeAsync(op)

        then:
        asList(replacement.getUpdate()) == getCollectionHelper().find(new BsonDocumentCodec())
    }

    def 'should fail if replacement contains an update operator'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1))), REPLACE)
                .upsert(true)
        def op = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, asList(replacement))

        when:
        op.execute(getBinding())

        then:
        thrown(IllegalArgumentException)
    }

    @Category(Slow)
    def 'should throw exception if document is too large'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16];
        def replacements = [new UpdateRequest(new BsonDocument(),
                                              new BsonDocument('_id', new BsonInt32(1)).append('b', new BsonBinary(hugeByteArray)),
                                              REPLACE)]

        when:
        new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, replacements).execute(getBinding())

        then:
        thrown(BsonSerializationException)
    }

    @Category(Slow)
    def 'should throw exception if document is too large asynchronously'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16];
        def replacements = [new UpdateRequest(new BsonDocument(),
                                              new BsonDocument('_id', new BsonInt32(1)).append('b', new BsonBinary(hugeByteArray)),
                                              REPLACE)]

        when:
        executeAsync(new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, replacements))

        then:
        def ex = thrown(MongoException)
        ex.getCause() instanceof BsonSerializationException
    }

    def 'should move _id to the beginning'() {
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        new InsertOperation(getNamespace(), true, ACKNOWLEDGED, asList(insert)).execute(getBinding())

        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('x', new BsonInt32(1)).append('_id', new BsonInt32(1)), REPLACE)
        def op = new UpdateOperation(getNamespace(), true, ACKNOWLEDGED, asList(replacement))

        when:
        op.execute(getBinding())

        then:
        getCollectionHelper().find().get(0).keySet() as List == ['_id', 'x']
    }
}