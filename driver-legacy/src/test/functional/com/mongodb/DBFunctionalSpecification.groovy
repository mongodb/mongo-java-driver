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

package com.mongodb

import org.bson.BsonDocument
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.configureFailPoint
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet

class DBFunctionalSpecification extends FunctionalSpecification {

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw WriteConcernException on write concern error for drop'() {
        given:
        database.createCollection('ctest', new BasicDBObject())

        def w = 2
        database.setWriteConcern(new WriteConcern(w))
        configureFailPoint(BsonDocument.parse('{ configureFailPoint: "failCommand", ' +
                'mode : {times : 1}, ' +
                'data : {failCommands : ["dropDatabase"], ' +
                'writeConcernError : {code : 100, errmsg : "failed"}}}'))

        when:
        database.dropDatabase()

        then:
        def e = thrown(WriteConcernException)
        e.getErrorCode() == 100

        cleanup:
        database.setWriteConcern(null)
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw WriteConcernException on write concern error for create collection'() {
        given:
        database.setWriteConcern(new WriteConcern(5))

        when:
        database.createCollection('ctest', new BasicDBObject())

        then:
        def e = thrown(WriteConcernException)
        e.getErrorCode() == 100

        cleanup:
        database.setWriteConcern(null)
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw WriteConcernException on write concern error for create view'() {
        given:
        database.setWriteConcern(new WriteConcern(5))

        when:
        database.createView('view1', 'collection1', [])

        then:
        def e = thrown(WriteConcernException)
        e.getErrorCode() == 100

        cleanup:
        database.setWriteConcern(null)
    }


    def 'should execute command with customer encoder'() {
        when:
        CommandResult commandResult = database.command(new BasicDBObject('isMaster', 1), DefaultDBEncoder.FACTORY.create())

        then:
        commandResult.ok()
    }
}
