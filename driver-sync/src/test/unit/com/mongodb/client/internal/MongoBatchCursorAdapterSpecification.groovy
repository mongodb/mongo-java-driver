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
import com.mongodb.internal.operation.BatchCursor
import org.bson.Document
import spock.lang.Specification


class MongoBatchCursorAdapterSpecification extends Specification {
    def 'should get server cursor and address'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def address = new ServerAddress('host', 27018)
        def serverCursor = new ServerCursor(5, address)
        batchCursor.getServerAddress() >> address
        batchCursor.getServerCursor() >> serverCursor
        def cursor = new MongoBatchCursorAdapter(batchCursor)

        expect:
        cursor.serverAddress.is(address)
        cursor.serverCursor.is(serverCursor)
    }

    def 'should throw on remove'() {
        given:
        def batchCursor = Stub(BatchCursor)
        def cursor = new MongoBatchCursorAdapter(batchCursor)

        when:
        cursor.remove()

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should close batch cursor'() {
        given:
        def batchCursor = Mock(BatchCursor)
        def cursor = new MongoBatchCursorAdapter(batchCursor)

        when:
        cursor.close()

        then:
        1 * batchCursor.close()
    }

    def 'should throw if closed'() {
        given:
        def batchCursor = Mock(BatchCursor)
        def cursor = new MongoBatchCursorAdapter(batchCursor)
        cursor.close()

        when:
        cursor.hasNext()

        then:
        thrown(IllegalStateException)

        when:
        cursor.next()

        then:
        thrown(IllegalStateException)

        when:
        cursor.tryNext()

        then:
        thrown(IllegalStateException)
    }

    def 'next should throw if there is no next'() {
        given:
        def batchCursor = Stub(BatchCursor)

        batchCursor.hasNext() >> false

        def cursor = new MongoBatchCursorAdapter(batchCursor)

        when:
        cursor.next()

        then:
        thrown(NoSuchElementException)
    }


    def 'should get next from batch cursor'() {
        given:
        def firstBatch = [new Document('x', 1), new Document('x', 1)]
        def secondBatch = [new Document('x', 2)]

        def batchCursor = Stub(BatchCursor)

        batchCursor.hasNext() >>> [true, true, true, true, false]
        batchCursor.next() >>> [firstBatch, secondBatch]

        def cursor = new MongoBatchCursorAdapter(batchCursor)

        expect:
        cursor.hasNext()
        cursor.next() == firstBatch[0]
        cursor.hasNext()
        cursor.next() == firstBatch[1]
        cursor.hasNext()
        cursor.next() == secondBatch[0]
        !cursor.hasNext()
    }

    def 'should try next from batch cursor'() {
        given:
        def firstBatch = [new Document('x', 1), new Document('x', 1)]
        def secondBatch = [new Document('x', 2)]

        def batchCursor = Stub(BatchCursor)

        batchCursor.tryNext() >>> [firstBatch, null, secondBatch, null]

        def cursor = new MongoBatchCursorAdapter(batchCursor)

        expect:
        cursor.tryNext() == firstBatch[0]
        cursor.tryNext() == firstBatch[1]
        cursor.tryNext() == null
        cursor.tryNext() == secondBatch[0]
        cursor.tryNext() == null
    }

    def 'should report available documents'() {
        given:
        def firstBatch = [new Document('x', 1), new Document('x', 1)]
        def secondBatch = [new Document('x', 2)]

        def batchCursor = Stub(BatchCursor)

        batchCursor.hasNext() >>> [true, true, true, true, false]
        batchCursor.next() >>> [firstBatch, secondBatch]
        batchCursor.available() >>> [2, 2, 0, 0, 0, 1, 0, 0, 0]

        when:
        def cursor = new MongoBatchCursorAdapter(batchCursor)

        then:
        cursor.available() == 2

        when:
        cursor.hasNext()

        then:
        cursor.available() == 2

        when:
        cursor.next()

        then:
        cursor.available() == 1

        when:
        cursor.hasNext()

        then:
        cursor.available() == 1

        when:
        cursor.next()

        then:
        cursor.available() == 0

        when:
        cursor.hasNext()

        then:
        cursor.available() == 1

        when:
        cursor.next()

        then:
        cursor.available() == 0    // fail

        when:
        cursor.hasNext()

        then:
        cursor.available() == 0

        when:
        cursor.close()

        then:
        cursor.available() == 0
    }
}
