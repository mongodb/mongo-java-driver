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
import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getOperationContext
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet

class CreateViewOperationSpecification extends OperationFunctionalSpecification {

    def 'should create view'() {
        given:
        def viewOn = getCollectionName()
        def viewName = getCollectionName() + '-view'
        def viewNamespace = new MongoNamespace(getDatabaseName(), viewName)

        assert !collectionNameExists(viewOn)
        assert !collectionNameExists(viewName)

        def trueXDocument = new BsonDocument('_id', new BsonInt32(1)).append('x', BsonBoolean.TRUE)
        def falseXDocument = new BsonDocument('_id', new BsonInt32(2)).append('x', BsonBoolean.FALSE)
        getCollectionHelper().insertDocuments([trueXDocument, falseXDocument])

        def pipeline = [new BsonDocument('$match', trueXDocument)]
        def operation = new CreateViewOperation(getDatabaseName(), viewName, viewOn, pipeline,
                WriteConcern.ACKNOWLEDGED)

        when:
        execute(operation, async)

        then:
        def options = getCollectionInfo(viewName).get('options')
        options.get('viewOn') == new BsonString(viewOn)
        options.get('pipeline') == new BsonArray(pipeline)
        getCollectionHelper(viewNamespace).find(new BsonDocumentCodec()) == [trueXDocument]

        cleanup:
        getCollectionHelper(viewNamespace).drop()

        where:
        async << [true, false]
    }

    def 'should create view with collation'() {
        given:
        def viewOn = getCollectionName()
        def viewName = getCollectionName() + '-view'
        def viewNamespace = new MongoNamespace(getDatabaseName(), viewName)

        assert !collectionNameExists(viewOn)
        assert !collectionNameExists(viewName)

        def operation = new CreateViewOperation(getDatabaseName(), viewName, viewOn, [],
                WriteConcern.ACKNOWLEDGED)
                .collation(defaultCollation)

        when:
        execute(operation, async)
        def collectionCollation = getCollectionInfo(viewName).get('options').get('collation')
        collectionCollation.remove('version')

        then:
        collectionCollation == defaultCollation.asDocument()

        cleanup:
        getCollectionHelper(viewNamespace).drop()

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        def viewName = getCollectionName() + '-view'
        def viewNamespace = new MongoNamespace(getDatabaseName(), viewName)
        assert !collectionNameExists(viewName)

        def operation = new CreateViewOperation(getDatabaseName(), viewName, getCollectionName(), [],
                new WriteConcern(5))

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        cleanup:
        getCollectionHelper(viewNamespace).drop()

        where:
        async << [true, false]
    }

    def getCollectionInfo(String collectionName) {
        def binding = getBinding()
        new ListCollectionsOperation(databaseName, new BsonDocumentCodec()).filter(new BsonDocument('name',
                new BsonString(collectionName))).execute(binding, getOperationContext(binding.getReadPreference())).tryNext()?.head()
    }

    def collectionNameExists(String collectionName) {
        getCollectionInfo(collectionName) != null
    }

}
