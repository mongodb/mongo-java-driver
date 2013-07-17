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
import org.mongodb.connection.Cluster
import org.mongodb.session.Session
import spock.lang.Specification
import spock.lang.Subject

import static com.mongodb.ReadPreference.primary
import static com.mongodb.WriteConcern.ACKNOWLEDGED

class DBCollectionSpecification extends Specification {
    private final Mongo mongo = Mock()
    private final DB database = new DB(mongo, 'myDatabase', new DocumentCodec())
    private final Session session = Mock()
    private final Cluster cluster = Mock()

    @Subject
    private final DBCollection collection = new DBCollection('collectionName', database, new DocumentCodec())

    def setup() {
        mongo.getCluster() >> { cluster }
        mongo.getSession() >> { session }

        //TODO: this shouldn't be required.  I think.
        database.setReadPreference(primary())
    }

    def 'should throw com.mongodb.MongoException if rename fails'() {
        given:
        session.execute(_) >> { throw new org.mongodb.MongoException('The error from the new Java layer') }

        when:
        collection.rename('newCollectionName');

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should throw com.mongodb.MongoException when update fails'() {
        given:
        session.execute(_) >> { throw new org.mongodb.MongoException('The error from the new Java layer') }

        when:
        collection.update(new BasicDBObject(), new BasicDBObject(), false, false, ACKNOWLEDGED);

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should throw MongoDuplicateKeyException when insert fails'() {
        given:
        session.execute(_) >> {
            throw new org.mongodb.command.MongoDuplicateKeyException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                         new org.mongodb.connection.ServerAddress(),
                                                                                         new Document(),
                                                                                         15L))
        }

        when:
        collection.insert(new BasicDBObject(), ACKNOWLEDGED);

        then:
        thrown(com.mongodb.MongoDuplicateKeyException)
    }

    def 'should wrap org.mongodb.MongoException as a com.mongodb.MongoException when insert fails'() {
        given:
        session.execute(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not escape') }

        when:
        collection.insert(new BasicDBObject(), ACKNOWLEDGED);

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should throw com.mongodb.CommandFailureException when group fails'() {
        given:
        session.execute(_) >> {
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

    def 'should throw MongoDuplicateKeyException when createIndex fails'() {
        given:
        session.execute(_) >> {
            throw new org.mongodb.command.MongoDuplicateKeyException(new org.mongodb.operation.CommandResult(new Document(),
                                                                                         new org.mongodb.connection.ServerAddress(),
                                                                                         new Document(),
                                                                                         15L))
        }

        when:
        collection.createIndex(new BasicDBObject());

        then:
        thrown(com.mongodb.MongoDuplicateKeyException)
    }

    def 'should wrap org.mongodb.MongoException as com.mongodb.MongoException when createIndex fails'() {
        given:
        session.execute(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not leak') }

        when:
        collection.createIndex(new BasicDBObject());

        then:
        thrown(MongoException)
    }

    def 'should throw com.mongodb.MongoException when drop fails'() {
        given:
        session.execute(_) >> {
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

    def 'should not throw com.mongodb.MongoException when ns not found'() {
        given:
        database.executeCommand(_) >> {
            org.mongodb.MongoException exception = new MongoCommandFailureException(new org.mongodb.operation.CommandResult(
                    new Document(),
                    new org.mongodb.connection.ServerAddress(),
                    new Document('errmsg','ns not found'),
                    15L));

            throw mapException(exception);
        }

        when:
        collection.drop();

        then:
        notThrown(com.mongodb.MongoException)
    }

    def 'should wrap org.mongodb.MongoException as a com.mongodb.MongoException for findAndModify'() {
        given:
        session.execute(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not escape') }

        when:
        collection.findAndModify(new BasicDBObject(), new BasicDBObject());

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should wrap org.mongodb.MongoException as a com.mongodb.MongoException for getIndexInfo'() {
        given:
        session.execute(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not escape') }

        when:
        collection.getIndexInfo();

        then:
        thrown(com.mongodb.MongoException)
    }

    //TODO: getSession.execute is used everywhere, maybe we should put this somewhere central and catch exceptions there?
    //TODO: should MongoInternalException map to internal exception?
    //TODO: remove doesn't declare an exception?
    //TODO: count declares it throws an exception, but doesn't?
    //TODO: check the message is correct
    //TODO: check the cause is correct
    //TODO: remove the org Exception from the cause
}
