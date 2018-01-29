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

package com.mongodb.client.internal

import com.mongodb.ServerAddress
import com.mongodb.ServerCursor
import com.mongodb.client.MongoCursor
import org.bson.Document
import spock.lang.Specification

class MongoMappingCursorSpecification extends Specification {
    def 'should get server cursor and address'() {
        given:
        def cursor = Stub(MongoCursor)
        def address = new ServerAddress('host', 27018)
        def serverCursor = new ServerCursor(5, address)
        cursor.getServerAddress() >> address
        cursor.getServerCursor() >> serverCursor
        def mappingCursor = new MongoMappingCursor(cursor, { })

        expect:
        mappingCursor.serverAddress.is(address)
        mappingCursor.serverCursor.is(serverCursor)
    }

    def 'should throw on remove'() {
        given:
        def cursor = Stub(MongoCursor)
        cursor.remove() >> { throw new UnsupportedOperationException() }
        def mappingCursor = new MongoMappingCursor(cursor, { })

        when:
        mappingCursor.remove()

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should close cursor'() {
        given:
        def cursor = Mock(MongoCursor)
        def mappingCursor = new MongoMappingCursor(cursor, { })

        when:
        mappingCursor.close()

        then:
        1 * cursor.close()
    }

    def 'should have next if cursor does'() {
        given:
        def cursor = Stub(MongoCursor)
        cursor.hasNext() >>> [true, false]
        def mappingCursor = new MongoMappingCursor(cursor, { })

        expect:
        mappingCursor.hasNext()
        !mappingCursor.hasNext()
    }

    def 'should map next'() {
        given:
        def cursor = Stub(MongoCursor)
        cursor.next() >> new Document('_id', 1)
        def mappingCursor = new MongoMappingCursor(cursor, { Document d -> d.get('_id') })

        expect:
        mappingCursor.next() == 1
    }

    def 'should map try next'() {
        given:
        def cursor = Stub(MongoCursor)
        cursor.tryNext() >>> [new Document('_id', 1), null]
        def mappingCursor = new MongoMappingCursor(cursor, { Document d -> d.get('_id') })

        expect:
        mappingCursor.tryNext() == 1
        !mappingCursor.tryNext()
    }
}
