/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.Document
import org.mongodb.codecs.DocumentCodec
import org.mongodb.command.Command
import org.mongodb.command.MongoCommandFailureException
import org.mongodb.command.Ping
import org.mongodb.connection.Cluster
import org.mongodb.connection.ClusterConnectionMode
import org.mongodb.connection.ClusterDescription
import org.mongodb.connection.MongoTimeoutException
import org.mongodb.session.Session
import org.mongodb.operation.MongoCursorNotFoundException
import org.mongodb.operation.ServerCursor
import spock.lang.Specification
import spock.lang.Subject

import static com.mongodb.ReadPreference.primary

class DBSpecification extends Specification {
    private final Session session = Mock()
    private final Cluster cluster = Mock()
    private final Mongo mongo = Mock()

    @Subject
    private final DB database = new DB(mongo, 'myDatabase', new DocumentCodec())

    def setup() {
        mongo.getCluster() >> { cluster }
        mongo.getSession() >> { session }
        //TODO: this shouldn't be required.  I think.
        database.setReadPreference(primary())
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    def 'should throw com.mongodb.MongoException if createCollection fails'() {
        given:
        cluster.getDescription() >> {new ClusterDescription(ClusterConnectionMode.Direct)}
        session.createServerConnectionProvider(_) >> {
            throw new MongoCommandFailureException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                           new org.mongodb.connection.ServerAddress(),
                                                                                           new Document(),
                                                                                           15L))
        }

        when:
        database.createCollection('myNewCollection', new BasicDBObject());

        then:
        thrown(com.mongodb.MongoException)
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    def 'should throw com.mongodb.MongoCursorNotFoundException if cursor not found'() {
        given:
        cluster.getDescription() >> {new ClusterDescription(ClusterConnectionMode.Direct)}
        session.createServerConnectionProvider(_) >> {
            throw new MongoCursorNotFoundException(new ServerCursor(1, new org.mongodb.connection.ServerAddress()))
        }

        when:
        database.executeCommand(new Command(new Document("isMaster", 1)));

        then:
        thrown(com.mongodb.MongoCursorNotFoundException)
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    def 'should throw com.mongodb.MongoException if executeCommand fails'() {
        given:
        cluster.getDescription() >> {new ClusterDescription(ClusterConnectionMode.Direct)}
        session.createServerConnectionProvider(_) >> {
            throw new MongoCommandFailureException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                           new org.mongodb.connection.ServerAddress(),
                                                                                           new Document(),
                                                                                           15L))
        }

        when:
        database.executeCommand(new Ping());

        then:
        thrown(com.mongodb.CommandFailureException)
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    def 'should throw com.mongodb.MongoException if getCollectionNames fails'() {
        given:
        session.createServerConnectionProvider(_) >> {
            throw new MongoCommandFailureException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                           new org.mongodb.connection.ServerAddress(),
                                                                                           new Document(),
                                                                                           15L))
        }

        when:
        database.getCollectionNames();

        then:
        thrown(com.mongodb.CommandFailureException)
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    def 'should throw com.mongodb.MongoException if command fails for a resons that is not a command failure'() {
        given:
        cluster.getDescription() >> {new ClusterDescription(ClusterConnectionMode.Direct)}
        session.createServerConnectionProvider(_) >> {
            throw new org.mongodb.MongoInternalException('An exception that is not a MongoCommandFailureException')
        }

        when:
        database.executeCommandAndReturnCommandResultIfCommandFailureException(new Ping())

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should not throw MongoCommandFailureException if command fails'() {
        given:
        def expectedCommandResult = new org.mongodb.operation.CommandResult(new Document(),
                                                                            new org.mongodb.connection.ServerAddress(),
                                                                            new Document(),
                                                                            15L)
        cluster.getDescription() >> {new ClusterDescription(ClusterConnectionMode.Direct)}
        session.createServerConnectionProvider(_) >> {
            throw new MongoCommandFailureException(expectedCommandResult)
        }

        when:
        org.mongodb.operation.CommandResult actualCommandResult = database.executeCommandAndReturnCommandResultIfCommandFailureException(
                new Ping())

        then:
        actualCommandResult == expectedCommandResult
    }

    @SuppressWarnings('UnnecessaryQualifiedReference')
    def 'should wrap org.mongodb.MongoException as com.mongodb.MongoException for getClusterDescription'() {
        given:
        cluster.getDescription() >> { throw new MongoTimeoutException('This Exception should not escape') }

        when:
        database.getClusterDescription()

        then:
        thrown(com.mongodb.MongoException)
    }

}
