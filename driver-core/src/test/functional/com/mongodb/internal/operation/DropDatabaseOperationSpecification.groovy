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

import com.mongodb.MongoWriteConcernException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import org.bson.BsonDocument
import org.bson.Document
import org.bson.codecs.DocumentCodec
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.configureFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded

class DropDatabaseOperationSpecification extends OperationFunctionalSpecification {

    @IgnoreIf({ isSharded() })
    def 'should drop a database that exists'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert databaseNameExists(databaseName)

        when:
        execute(new DropDatabaseOperation(databaseName, WriteConcern.ACKNOWLEDGED), async)

        then:
        !databaseNameExists(databaseName)

        where:
        async << [true, false]
    }


    def 'should not error when dropping a collection that does not exist'() {
        given:
        def dbName = 'nonExistingDatabase'

        when:
        execute(new DropDatabaseOperation(dbName, WriteConcern.ACKNOWLEDGED), async)

        then:
        !databaseNameExists(dbName)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))

        def w = 2
        def operation = new DropDatabaseOperation(databaseName, new WriteConcern(w))
        configureFailPoint(BsonDocument.parse('{ configureFailPoint: "failCommand", ' +
                'mode : {times : 1}, ' +
                'data : {failCommands : ["dropDatabase"], ' +
                'writeConcernError : {code : 100, errmsg : "failed"}}}'))

        when:
        async ? executeAsync(operation) : operation.execute(getBinding())

        then:
        def ex = thrown(MongoWriteConcernException)
        ex.writeConcernError.code == 100
        ex.writeResult.wasAcknowledged()

        where:
        async << [true, false]
    }

    def databaseNameExists(String databaseName) {
        new ListDatabasesOperation(new DocumentCodec()).execute(getBinding()).next()*.name.contains(databaseName)
    }

}
