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
import org.mongodb.operation.MongoQueryFailureException
import spock.lang.Specification
import spock.lang.Subject

import static com.mongodb.ReadPreference.PRIMARY

class DBCursorSpecification extends Specification {
    private final org.mongodb.session.Session session = Mock()
    private final DBCollection collection = Mock();

    @Subject
    private final DBCursor dbCursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), PRIMARY);

    def setup() {
        collection.getSession() >> { session }
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
        session.createServerConnectionProvider(_) >> { throw new MongoQueryFailureException(null, new Document('code', 123)) }

        when:
        dbCursor.hasNext()

        then:
        thrown(MongoException)
    }
}
