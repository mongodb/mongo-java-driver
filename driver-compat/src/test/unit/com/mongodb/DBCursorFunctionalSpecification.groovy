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

import spock.lang.Ignore
import spock.lang.Subject

class DBCursorFunctionalSpecification extends FunctionalSpecification {

    @Subject
    private DBCursor dbCursor

    def setup() {
        collection.insert(new BasicDBObject('a', 1))
    }

    def 'should use provided decoder factory'() {
        given:
        DBDecoder decoder = Mock()
        DBDecoderFactory factory = Mock()
        factory.create() >> decoder

        when:
        dbCursor = collection.find()
        dbCursor.setDecoderFactory(factory)
        dbCursor.next()

        then:
        1 * decoder.decode(_ as byte[], collection)
    }

    def 'should use provided hints for queries'() {
        given:
        collection.createIndex(new BasicDBObject('a', 1))

        when:
        dbCursor = collection.find().hint(new BasicDBObject('a', 1))
        def explainDocument = dbCursor.explain()

        then:
        explainDocument.get('cursor') == 'BtreeCursor a_1'
    }

    def 'should return results in the order they are on disk when natural sort applied'() {
        given:
        collection.insert(new BasicDBObject('name', 'Chris'))
        collection.insert(new BasicDBObject('name', 'Adam'))
        collection.insert(new BasicDBObject('name', 'Bob'))

        when:
        DBCursor sortedCollection = collection.find(new BasicDBObject('name', new BasicDBObject('$exists', true)))
                                              .sort(new BasicDBObject('$natural', 1))

        then:
        sortedCollection.next().get('name') == 'Chris'
        sortedCollection.next().get('name') == 'Adam'
        sortedCollection.next().get('name') == 'Bob'
    }

    def 'should return results in the reverse order they are on disk when natural sort of minus one applied'() {
        given:
        collection.insert(new BasicDBObject('name', 'Chris'))
        collection.insert(new BasicDBObject('name', 'Adam'))
        collection.insert(new BasicDBObject('name', 'Bob'))

        when:
        DBCursor sortedCollection = collection.find(new BasicDBObject('name', new BasicDBObject('$exists', true)))
                                              .sort(new BasicDBObject('$natural', -1))

        then:
        sortedCollection.next().get('name') == 'Bob'
        sortedCollection.next().get('name') == 'Adam'
        sortedCollection.next().get('name') == 'Chris'
    }

    def 'should sort in reverse order'() {
        given:
        for (i in 1..10) {
            collection.insert(new BasicDBObject('x', i))
        }

        when:
        def cursor = collection.find()
                               .sort(new BasicDBObject('x', -1))

        then:
        cursor.next().get('x') == 10
    }

    def 'should sort in order'() {
        given:
        for (i in 89..80) {
            def document = new BasicDBObject('x', i)
            collection.insert(document)
        }

        when:
        def cursor = collection.find(new BasicDBObject('x', new BasicDBObject('$exists', true)))
                               .sort(new BasicDBObject('x', 1))

        then:
        cursor.next().get('x') == 80
    }

    @Ignore('Currently the DBObjectCodec encodes ID first, so any sort criteria containing ID not in the first position will be broken')
    def 'should sort on two fields'() {
        given:
        collection.insert(new BasicDBObject('_id', 1).append('name', 'Chris'))
        collection.insert(new BasicDBObject('_id', 2).append('name', 'Adam'))
        collection.insert(new BasicDBObject('_id', 3).append('name', 'Bob'))
        collection.insert(new BasicDBObject('_id', 5).append('name', 'Adam'))
        collection.insert(new BasicDBObject('_id', 4).append('name', 'Adam'))

        when:
        def cursor = collection.find(new BasicDBObject('name', new BasicDBObject('$exists', true)))
                               .sort(new BasicDBObject('name', 1).append('_id', 1))

        then:
        def first = cursor.next()
        first.get('name') == 'Adam'
        first.get('_id') == 2
        def second = cursor.next()
        second.get('name') == 'Adam'
        second.get('_id') == 4
        def third = cursor.next()
        third.get('name') == 'Adam'
        third.get('_id') == 5
        def fourth = cursor.next()
        fourth.get('name') == 'Bob'
        fourth.get('_id') == 3
        def fifth = cursor.next()
        fifth.get('name') == 'Chris'
        fifth.get('_id') == 1
    }
}
