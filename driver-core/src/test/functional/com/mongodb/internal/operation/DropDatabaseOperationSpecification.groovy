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

import static com.mongodb.ClusterFixture.CSOT_TIMEOUT
import static com.mongodb.ClusterFixture.configureFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast

class DropDatabaseOperationSpecification extends OperationFunctionalSpecification {

    @IgnoreIf({ isSharded() })
    def 'should drop a database that exists'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert databaseNameExists(databaseName)

        when:
        new DropDatabaseOperation(CSOT_TIMEOUT, databaseName).execute(getBinding())

        then:
        !databaseNameExists(databaseName)
    }


    @IgnoreIf({ isSharded() })
    def 'should drop a database that exists asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert databaseNameExists(databaseName)

        when:
        executeAsync(new DropDatabaseOperation(CSOT_TIMEOUT, databaseName))

        then:
        !databaseNameExists(databaseName)
    }

    def 'should not error when dropping a collection that does not exist'() {
        given:
        def dbName = 'nonExistingDatabase'

        when:
        new DropDatabaseOperation(CSOT_TIMEOUT, dbName).execute(getBinding())

        then:
        !databaseNameExists(dbName)
    }


    def 'should not error when dropping a collection that does not exist asynchronously'() {
        given:
        def dbName = 'nonExistingDatabase'

        when:
        executeAsync(new DropDatabaseOperation(CSOT_TIMEOUT, dbName))

        then:
        !databaseNameExists(dbName)
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))

        // On servers older than 4.0 that don't support this failpoint, use a crazy w value instead
        def w = serverVersionAtLeast(4, 0) ? 2 : 5
        def operation = new DropDatabaseOperation(CSOT_TIMEOUT, databaseName, new WriteConcern(w))
        if (serverVersionAtLeast(4, 0)) {
            configureFailPoint(BsonDocument.parse('{ configureFailPoint: "failCommand", ' +
                    'mode : {times : 1}, ' +
                    'data : {failCommands : ["dropDatabase"], ' +
                    'writeConcernError : {code : 100, errmsg : "failed"}}}'))
        }

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
        new ListDatabasesOperation(CSOT_TIMEOUT, new DocumentCodec()).execute(getBinding()).next()*.name.contains(databaseName);
    }

}
