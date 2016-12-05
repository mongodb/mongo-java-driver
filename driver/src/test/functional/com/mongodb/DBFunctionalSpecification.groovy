/*
 * Copyright (c) 2008-2016 MongoDB, Inc.
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

import com.mongodb.operation.OperationExecutor
import org.bson.BsonDocument
import org.bson.BsonDouble
import org.bson.BsonInt32
import org.bson.BsonString
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.Fixture.getMongoClient

class DBFunctionalSpecification extends FunctionalSpecification {

    def 'DB addUser should work around localhost exception issues'() {
        given:
        def executor = Stub(OperationExecutor) {
            execute(_, _) >> {
                throw new MongoCommandException(new BsonDocument('ok', new BsonDouble(0))
                                                          .append('code', new BsonInt32(13))
                                                          .append('errmsg', new BsonString('not authorized on admin to execute ' +
                                                                                           'command { usersInfo: "admin" }')),
                                                  new ServerAddress())
            }

            execute(_) >> null
        }
        def database = new DB(getMongoClient(), 'admin', executor)

        when:
        def result = database.addUser('ross', 'pwd'.toCharArray())

        then:
        notThrown(MongoCommandException)
        !result.updateOfExisting
        result.getN() == 1
    }

    def 'DB addUser should throw non localhost exception issues'() {
        given:
        def executor = Stub(OperationExecutor) {
            execute(_, _) >> { throw new MongoException('Some error') }
        }
        def database = new DB(getMongoClient(), 'admin', executor)

        when:
        database.addUser('ross', 'pwd'.toCharArray())

        then:
        thrown(MongoException)

        when:
        executor = Stub(OperationExecutor) {
            execute(_, _) >> {
                throw new MongoCommandException(new BsonDocument('ok', new BsonDouble(0))
                                                          .append('code', new BsonInt32(13))
                                                          .append('errmsg', new BsonString('not authorized on admin to execute ' +
                                                                                           'command { usersInfo: "admin" }')),
                                                  new ServerAddress())
            }

            execute(_) >> { throw new MongoException('Some error') }
        }
        database = new DB(getMongoClient(), 'admin', executor)
        database.addUser('ross', 'pwd'.toCharArray())

        then:
        thrown(MongoException)
    }


    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw WriteConcernException on write concern error for drop'() {
        given:
        database.createCollection('ctest', new BasicDBObject())
        database.setWriteConcern(new WriteConcern(5))

        when:
        database.dropDatabase()

        then:
        def e = thrown(WriteConcernException)
        e.getErrorCode() == 100

        cleanup:
        database.setWriteConcern(null)
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
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

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
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

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw WriteConcernException on write concern error for add user'() {
        given:
        database.setWriteConcern(new WriteConcern(5))

        when:
        database.addUser('writeConcernUser', 'foo'.toCharArray())

        then:
        def e = thrown(WriteConcernException)
        e.getErrorCode() == 100

        cleanup:
        database.setWriteConcern(null)
        database.removeUser('writeConcernUser')
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || !isDiscoverableReplicaSet() })
    def 'should throw WriteConcernException on write concern error for remove user'() {
        given:
        database.addUser('writeConcernUser', 'foo'.toCharArray())
        database.setWriteConcern(new WriteConcern(5))

        when:
        database.removeUser('writeConcernUser')

        then:
        def e = thrown(WriteConcernException)
        e.getErrorCode() == 100

        cleanup:
        database.setWriteConcern(null)
    }
}
