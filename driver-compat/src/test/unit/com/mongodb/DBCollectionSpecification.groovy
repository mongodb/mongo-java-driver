/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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
import org.mongodb.connection.Cluster
import org.mongodb.connection.ClusterConnectionMode
import org.mongodb.connection.ClusterDescription
import org.mongodb.connection.ClusterType
import org.mongodb.session.Session
import spock.lang.Specification
import spock.lang.Subject

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.concurrent.TimeUnit.SECONDS
import static org.mongodb.Fixture.getBufferProvider

//we want to explicitly state the namespaces of the exceptions that are thrown
@SuppressWarnings('UnnecessaryQualifiedReference')
class DBCollectionSpecification extends Specification {
    private final DB database = Mock(DB)
    private final Session session = Mock(Session)
    private final Cluster cluster = Mock(Cluster)

    @Subject
    private final DBCollection collection = new DBCollection('collectionName', database, new DocumentCodec())

    def setup() {
        database.getSession() >> { session }
        database.getCluster() >> { cluster }
        database.getName() >> { 'TheDatabase' }
        database.getClusterDescription() >> { cluster.getDescription(10, SECONDS) }
        database.getBufferPool() >> { getBufferProvider() }
        cluster.getDescription(10, SECONDS) >> { new ClusterDescription(ClusterConnectionMode.MULTIPLE, ClusterType.UNKNOWN, []) }
    }

    def 'should throw com.mongodb.MongoException if rename fails'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new org.mongodb.MongoException('The error from the new Java layer') }

        when:
        collection.rename('newCollectionName');

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should throw com.mongodb.MongoException when update fails'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new org.mongodb.MongoException('The error from the new Java layer') }

        when:
        collection.update(new BasicDBObject(), new BasicDBObject(), false, false, ACKNOWLEDGED);

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should throw MongoDuplicateKeyException when insert fails'() {
        given:
        session.createServerConnectionProvider(_) >> {
            throw new org.mongodb.MongoDuplicateKeyException(11000, 'duplicate key error', new org.mongodb.CommandResult(
                    new org.mongodb.connection.ServerAddress(), new Document(), 15L))
        }

        when:
        collection.insert(new BasicDBObject(), ACKNOWLEDGED);

        then:
        thrown(DuplicateKeyException)
    }

    def 'should wrap org.mongodb.MongoException as a com.mongodb.MongoException when insert fails'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not escape') }

        when:
        collection.insert(new BasicDBObject(), ACKNOWLEDGED);

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should throw MongoDuplicateKeyException when createIndex fails'() {
        given:
        session.createServerConnectionProvider(_) >> {
            throw new org.mongodb.MongoDuplicateKeyException(11000, 'duplicate key error', new org.mongodb.CommandResult(
                    new org.mongodb.connection.ServerAddress(), new Document(), 15L))
        }

        when:
        collection.createIndex(new BasicDBObject());

        then:
        thrown(DuplicateKeyException)
    }

    def 'should wrap org.mongodb.MongoException as com.mongodb.MongoException when createIndex fails'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not leak') }

        when:
        collection.createIndex(new BasicDBObject());

        then:
        thrown(MongoException)
    }

    def 'should throw com.mongodb.MongoException when drop fails'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not escape') }

        when:
        collection.drop();

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should wrap org.mongodb.MongoException as a com.mongodb.MongoException for findAndModify'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not escape') }

        when:
        collection.findAndModify(new BasicDBObject(), new BasicDBObject());

        then:
        thrown(com.mongodb.MongoException)
    }

    def 'should wrap org.mongodb.MongoException as a com.mongodb.MongoException for getIndexInfo'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not escape') }

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
