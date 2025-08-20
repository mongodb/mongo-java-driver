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


import com.mongodb.MongoNamespace
import com.mongodb.MongoServerException
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getOperationContext
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded

@IgnoreIf( { isSharded() } )  // these tests don't reliably pass against mongos
class RenameCollectionOperationSpecification extends OperationFunctionalSpecification {

    def cleanup() {
        def binding = getBinding()
        new DropCollectionOperation(new MongoNamespace(getDatabaseName(), 'newCollection'),
                WriteConcern.ACKNOWLEDGED).execute(binding, getOperationContext(binding.getReadPreference()))
    }

    def 'should return rename a collection'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))
        assert collectionNameExists(getCollectionName())
        def operation = new RenameCollectionOperation(getNamespace(),
                new MongoNamespace(getDatabaseName(), 'newCollection'), null)

        when:
        execute(operation, async)

        then:
        !collectionNameExists(getCollectionName())
        collectionNameExists('newCollection')

        where:
        async << [true, false]
    }

    def 'should throw if not drop and collection exists'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))
        assert collectionNameExists(getCollectionName())
        def operation = new RenameCollectionOperation(getNamespace(), getNamespace(), null)

        when:
        execute(operation, async)

        then:
        thrown(MongoServerException)
        collectionNameExists(getCollectionName())

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentThat', 'forces creation of the Collection'))
        assert collectionNameExists(getCollectionName())
        def operation = new RenameCollectionOperation(getNamespace(),
                new MongoNamespace(getDatabaseName(), 'newCollection'), new WriteConcern(5))


        def binding = getBinding()
        when:
        async ? executeAsync(operation) : operation.execute(binding, getOperationContext(binding.getReadPreference()))

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def collectionNameExists(String collectionName) {
        def binding = getBinding()
        def cursor = new ListCollectionsOperation(databaseName, new DocumentCodec()).execute(binding,
                getOperationContext(binding.getReadPreference()))
        if (!cursor.hasNext()) {
            return false
        }
        cursor.next()*.get('name').contains(collectionName)
    }

}
