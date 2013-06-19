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
import org.mongodb.command.MongoCommandFailureException
import org.mongodb.command.MongoDuplicateKeyException
import org.mongodb.connection.Cluster
import org.mongodb.session.ServerSelectingSession
import spock.lang.Specification
import spock.lang.Subject

import static com.mongodb.WriteConcern.ACKNOWLEDGED

class DBCollectionSpecification extends Specification {
    private final DB database = Mock();
    private final ServerSelectingSession session = Mock()
    private final Cluster cluster = Mock()

    @Subject
    private final DBCollection collection = new DBCollection('collectionName', database, new DocumentCodec())

    def setup() {
        // these are here to prevent null pointers more than anything
        database.getSession() >> { session }
        database.getCluster() >> { cluster }
        database.getName() >> { 'TheDatabase' }
    }

    def 'should throw com.mongodb.MongoException if rename fails'() {
        setup:
        session.execute(_) >> { throw new org.mongodb.MongoException('The error from the new Java layer') }

        when:
        collection.rename('newCollectionName');

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should throw com.mongodb.MongoException when update fails'() {
        setup:
        session.execute(_) >> { throw new org.mongodb.MongoException('The error from the new Java layer') }

        when:
        collection.update(new BasicDBObject(), new BasicDBObject(), false, false, ACKNOWLEDGED);

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should throw MongoException.DuplicateKey when insert fails'() {
        setup:
        session.execute(_) >> {
            throw new MongoDuplicateKeyException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                         new org.mongodb.connection.ServerAddress(),
                                                                                         new Document(),
                                                                                         15L))
        }

        when:
        collection.insert(new BasicDBObject(), ACKNOWLEDGED);

        then:
        thrown(com.mongodb.MongoException.DuplicateKey)
        //TODO: this is failing cos it's ignoring the write concern?
    }

    def 'should throw com.mongodb.CommandFailureException when group fails'() {
        setup:
        database.executeCommand(_) >> {
            throw new MongoCommandFailureException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                           new org.mongodb.connection.ServerAddress(),
                                                                                           new Document(),
                                                                                           15L))
        }

        when:
        collection.group(new BasicDBObject());

        then:
        thrown(com.mongodb.CommandFailureException)
    }

    def 'should throw MongoException.DuplicateKey when createIndex fails'() {
        setup:
        session.execute(_) >> {
            throw new MongoDuplicateKeyException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                         new org.mongodb.connection.ServerAddress(),
                                                                                         new Document(),
                                                                                         15L))
        }

        when:
        collection.createIndex(new BasicDBObject());

        then:
        thrown(MongoException.DuplicateKey)
    }

    def 'should throw com.mongodb.MongoException when drop fails'() {
        setup:
        database.executeCommand(_) >> {
            throw new MongoCommandFailureException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                           new org.mongodb.connection.ServerAddress(),
                                                                                           new Document(),
                                                                                           15L))
        }

        when:
        collection.drop();

        then:
        thrown(com.mongodb.MongoException)
    }

    //TODO: getIndexInfo declares it throws an Exception, and doesn't?
    //TODO: remove doesn't declare an exception?
    //TODO: count declares it throws an exception, but doesn't?
    //TODO: turn org exception into checked exception to see if anything's leaking
    //TODO: check the message is correct
    //TODO: check the cause is correct
}
