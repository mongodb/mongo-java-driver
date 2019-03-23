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

import com.mongodb.MongoException
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf
import spock.lang.Unroll

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.concurrent.TimeUnit.SECONDS

class DropIndexOperationSpecification extends OperationFunctionalSpecification {

    def 'should not error when dropping non-existent index on non-existent collection'() {
        when:
        execute(new DropIndexOperation(getNamespace(), 'made_up_index_1'), async)

        then:
        getIndexes().size() == 0

        where:
        async << [true, false]
    }

    def 'should error when dropping non-existent index on existing collection'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))

        when:
        execute(new DropIndexOperation(getNamespace(), 'made_up_index_1'), async)

        then:
        thrown(MongoException)

        where:
        async << [true, false]
    }

    def 'should drop existing index by name'() {
        given:
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))

        when:
        execute(new DropIndexOperation(getNamespace(), 'theField_1'), async)
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'

        where:
        async << [true, false]
    }

    @Unroll
    def 'should drop existing index by keys'() {
        given:
        collectionHelper.createIndex(keys)

        when:
        execute(new DropIndexOperation(getNamespace(), keys), async)
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'

        where:
        [keys, async] << [
                [new BsonDocument('theField', new BsonInt32(1)),
                 new BsonDocument('theField', new BsonInt32(1)).append('theSecondField', new BsonInt32(-1)),
                 new BsonDocument('theField', new BsonString('2d')),
                 new BsonDocument('theField', new BsonString('hashed')),
                ],
                [true, false]
        ].combinations()
    }

    @IgnoreIf({ isSharded() })
    def 'should throw execution timeout exception from execute'() {
        given:
        def keys = new BsonDocument('theField', new BsonInt32(1))
        collectionHelper.createIndex(keys)
        def operation = new DropIndexOperation(getNamespace(), keys).maxTime(30, SECONDS)

        enableMaxTimeFailPoint()

        when:
        execute(operation, async)

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()

        where:
        async << [true, false]
    }

    def 'should drop existing index by key when using BsonInt64'() {
        given:
        def keys = new BsonDocument('theField', new BsonInt32(1))
        collectionHelper.createIndex(keys)

        when:
        execute(new DropIndexOperation(getNamespace(), new BsonDocument('theField', new BsonInt64(1))), async)
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'

        where:
        async << [true, false]
    }

    def 'should drop all indexes when passed *'() {
        given:
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        collectionHelper.createIndex(new BsonDocument('theOtherField', new BsonInt32(1)))

        when:
        execute(new DropIndexOperation(getNamespace(), '*'), async)
        List<Document> indexes = getIndexes()

        then:
        indexes.size() == 1
        indexes[0].name == '_id_'

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        collectionHelper.createIndex(new BsonDocument('theField', new BsonInt32(1)))
        def operation = new DropIndexOperation(getNamespace(), 'theField_1', new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def getIndexes() {
        def indexes = []
        def cursor = new ListIndexesOperation(getNamespace(), new DocumentCodec()).execute(getBinding())
        while (cursor.hasNext()) {
            indexes.addAll(cursor.next())
        }
        indexes
    }

}
