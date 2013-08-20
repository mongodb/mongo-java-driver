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

import spock.lang.Subject

class DBCursorFunctionalSpecification extends FunctionalSpecification {

    @Subject
    private DBCursor dbCursor;

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

    def 'should use provided in collection decoder factory'() {
        given:
        DBDecoder decoder = Mock()
        DBDecoderFactory factory = Mock()
        factory.create() >> decoder

        when:
        collection.setDBDecoderFactory(factory)
        dbCursor = collection.find()
        dbCursor.next()

        then:
        1 * decoder.decode(_ as byte[], collection)
    }

    def 'should use provided hints for queries'() {
        given:
        collection.ensureIndex(new BasicDBObject('a',1))

        when:
        dbCursor = collection.find().hint(new BasicDBObject('a', 1))

        then:
        dbCursor.explain().get('cursor') == 'BtreeCursor a_1'
    }

}
