/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.operation

import com.mongodb.MongoClientException
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
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast

class CreateViewOperationSpecification extends OperationFunctionalSpecification {

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should create view'() {
        given:
        def viewOn = getCollectionName();
        def viewName = getCollectionName() + '-view'
        def viewNamespace = new MongoNamespace(getDatabaseName(), viewName)

        assert !collectionNameExists(viewOn)
        assert !collectionNameExists(viewName)

        def trueXDocument = new BsonDocument('_id', new BsonInt32(1)).append('x', BsonBoolean.TRUE)
        def falseXDocument = new BsonDocument('_id', new BsonInt32(2)).append('x', BsonBoolean.FALSE)
        getCollectionHelper().insertDocuments([trueXDocument, falseXDocument])

        def pipeline = [new BsonDocument('$match', trueXDocument)]
        def operation = new CreateViewOperation(getDatabaseName(), viewName, viewOn, pipeline, WriteConcern.ACKNOWLEDGED)

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

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should create view with collation'() {
        given:
        def viewOn = getCollectionName();
        def viewName = getCollectionName() + '-view'
        def viewNamespace = new MongoNamespace(getDatabaseName(), viewName)

        assert !collectionNameExists(viewOn)
        assert !collectionNameExists(viewName)

        def operation = new CreateViewOperation(getDatabaseName(), viewName, viewOn, [], WriteConcern.ACKNOWLEDGED)
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

    @IgnoreIf({ serverVersionAtLeast(3, 4) })
    def 'should throw if server version is not 3.4 or greater'() {
        given:
        def operation = new CreateViewOperation(getDatabaseName(), getCollectionName() + '-view',
                getCollectionName(), [], WriteConcern.ACKNOWLEDGED)

        when:
        execute(operation, async)

        then:
        thrown(MongoClientException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        def viewName = getCollectionName() + '-view'
        def viewNamespace = new MongoNamespace(getDatabaseName(), viewName)
        assert !collectionNameExists(viewName)

        def operation = new CreateViewOperation(getDatabaseName(), viewName, getCollectionName(), [], new WriteConcern(5))

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
        new ListCollectionsOperation(databaseName, new BsonDocumentCodec()).filter(new BsonDocument('name',
                new BsonString(collectionName))).execute(getBinding()).tryNext()?.head()
    }

    def collectionNameExists(String collectionName) {
        getCollectionInfo(collectionName) != null
    }

}
