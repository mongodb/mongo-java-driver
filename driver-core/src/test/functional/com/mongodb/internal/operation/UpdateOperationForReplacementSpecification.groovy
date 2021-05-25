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

import com.mongodb.OperationFunctionalSpecification
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.bulk.UpdateRequest
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonSerializationException
import org.bson.codecs.BsonDocumentCodec
import spock.lang.IgnoreIf
import util.spock.annotations.Slow

import static com.mongodb.ClusterFixture.CSOT_TIMEOUT
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE
import static java.util.Arrays.asList

class UpdateOperationForReplacementSpecification extends OperationFunctionalSpecification {

    def 'should return correct result'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument(), new BsonDocument('_id', new BsonInt32(1)), REPLACE)
        def operation = new UpdateOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        def result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()

        where:
        async << [true, false]
    }

    def 'should replace a single document'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        new InsertOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(insert)).execute(getBinding())

        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
        def operation = new UpdateOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        def result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == null
        result.isUpdateOfExisting()
        asList(replacement.getUpdateValue()) == getCollectionHelper().find(new BsonDocumentCodec())
        getCollectionHelper().find().get(0).keySet().iterator().next() == '_id'

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(4, 2) })
    def 'should support hint'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        new InsertOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(insert)).execute(getBinding())

        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
                .hint(hint)
                .hintString(hintString)
        def operation = new UpdateOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        def result = execute(operation, async)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == null
        result.isUpdateOfExisting()
        asList(replacement.getUpdateValue()) == getCollectionHelper().find(new BsonDocumentCodec())
        getCollectionHelper().find().get(0).keySet().iterator().next() == '_id'

        where:
        [async, hint, hintString] << [
                [true, false],
                [null, new BsonDocument('_id', new BsonInt32(1))],
                [null, '_id_']
        ].combinations()
    }

    def 'should upsert a single document'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
                .upsert(true)
        def operation = new UpdateOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        execute(operation, async)

        then:
        asList(replacement.getUpdateValue()) == getCollectionHelper().find(new BsonDocumentCodec())

        where:
        async << [true, false]
    }

    def 'should fail if replacement contains an update operator'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1))), REPLACE)
                .upsert(true)
        def operation = new UpdateOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        execute(operation, async)

        then:
        thrown(IllegalArgumentException)

        where:
        async << [true, false]
    }

    @Slow
    def 'should throw exception if document is too large'() {
        given:
        byte[] hugeByteArray = new byte[(1024 * 1024 * 16) + 16 * 1024]
        def replacements = [new UpdateRequest(new BsonDocument(),
                                              new BsonDocument('_id', new BsonInt32(1)).append('b', new BsonBinary(hugeByteArray)),
                                              REPLACE)]
        def operation = new UpdateOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, replacements)

        when:
        execute(operation, async)

        then:
        thrown(BsonSerializationException)

        where:
        async << [true, false]
    }

    def 'should move _id to the beginning'() {
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        new InsertOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(insert)).execute(getBinding())

        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                            new BsonDocument('x', new BsonInt32(1)).append('_id', new BsonInt32(1)), REPLACE)
        def operation = new UpdateOperation(CSOT_TIMEOUT, getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        execute(operation, async)

        then:
        getCollectionHelper().find().get(0).keySet() as List == ['_id', 'x']

        where:
        async << [true, false]
    }
}
