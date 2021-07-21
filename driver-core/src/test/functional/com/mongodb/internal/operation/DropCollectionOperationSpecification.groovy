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
import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionLessThan

class DropCollectionOperationSpecification extends OperationFunctionalSpecification {

    def 'should drop a collection that exists'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert collectionNameExists(getCollectionName())

        when:
        new DropCollectionOperation(getNamespace()).execute(getBinding())

        then:
        !collectionNameExists(getCollectionName())
    }


    def 'should drop a collection that exists asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert collectionNameExists(getCollectionName())

        when:
        executeAsync(new DropCollectionOperation(getNamespace()))

        then:
        !collectionNameExists(getCollectionName())
    }

    def 'should not error when dropping a collection that does not exist'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'nonExistingCollection')

        when:
        new DropCollectionOperation(namespace).execute(getBinding())

        then:
        !collectionNameExists('nonExistingCollection')
    }


    def 'should not error when dropping a collection that does not exist asynchronously'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'nonExistingCollection')

        when:
        executeAsync(new DropCollectionOperation(namespace))

        then:
        !collectionNameExists('nonExistingCollection')
    }

    @IgnoreIf({ serverVersionLessThan(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert collectionNameExists(getCollectionName())
        def operation = new DropCollectionOperation(getNamespace(), new WriteConcern(5))

        when:
        async ? executeAsync(operation) : operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def collectionNameExists(String collectionName) {
        def cursor = new ListCollectionsOperation(databaseName, new DocumentCodec()).execute(getBinding())
        if (!cursor.hasNext()) {
            return false
        }
        cursor.next()*.get('name').contains(collectionName)
    }

}
