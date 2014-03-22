/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import org.mongodb.MongoNamespace
import org.mongodb.MongoQueryFailureException
import org.mongodb.SimpleBufferProvider
import org.mongodb.codecs.ObjectIdGenerator
import org.mongodb.codecs.PrimitiveCodecs
import org.mongodb.session.Session
import spock.lang.Specification
import spock.lang.Subject

import static com.mongodb.Fixture.getPrimary

class DBCursorSpecification extends Specification {
    private final Session session = Mock()
    private final Mongo mongo = Mock()
    private final DB db = Mock()
    private final DBCollection collection = Mock()

    @Subject
    private DBCursor dbCursor

    def setup() {
        collection.getDocumentCodec() >> { new DocumentCodec(PrimitiveCodecs.createDefault()) }
        collection.getNamespace() >> { new MongoNamespace('test', 'test') }
        collection.getSession() >> { session }
        collection.getBufferPool() >> { new SimpleBufferProvider() }
        collection.getDB() >> { db }
        collection.getObjectCodec() >> { new CollectibleDBObjectCodec(db,
                                                                      PrimitiveCodecs.createDefault(),
                                                                      new ObjectIdGenerator(),
                                                                      new DBObjectFactory()); }
        db.getMongo() >> { mongo }
        mongo.getMongoClientOptions() >> { MongoClientOptions.builder().build() }
        dbCursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary());
    }

    def 'should wrap org.mongodb.MongoException with com.mongodb.MongoException for errors in explain'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new org.mongodb.MongoInternalException('Exception that should not escape') }

        when:
        dbCursor.explain()

        then:
        thrown(MongoException)
    }

    def 'should wrap org.mongodb.MongoException with com.mongodb.MongoException for errors in hasNext'() {
        given:
        session.createServerConnectionProvider(_) >> { throw new MongoQueryFailureException(getPrimary().toNew(), 50, 'error') }

        when:
        dbCursor.hasNext()

        then:
        thrown(MongoException)
    }
}
