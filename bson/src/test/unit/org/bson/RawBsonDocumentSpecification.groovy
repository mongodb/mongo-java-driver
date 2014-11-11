/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson

import org.bson.codecs.BsonDocumentCodec
import spock.lang.Specification

import static java.util.Arrays.asList

class RawBsonDocumentSpecification extends Specification {

    def emptyDocument = new BsonDocument()
    def emptyRawDocument = new RawBsonDocument(emptyDocument, new BsonDocumentCodec());
    def document = new BsonDocument()
            .append('a', new BsonInt32(1))
            .append('b', new BsonInt32(2))
            .append('c', new BsonDocument('x', BsonBoolean.TRUE))
            .append('d', new BsonArray(asList(new BsonDocument('y', BsonBoolean.FALSE), new BsonInt32(1))))

    def rawDocument = new RawBsonDocument(document, new BsonDocumentCodec())

    def 'containKey should throw if the key name is null'() {
        when:
        rawDocument.containsKey(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'containsKey should find an existing key'() {
        expect:
        rawDocument.containsKey('a')
        rawDocument.containsKey('b')
        rawDocument.containsKey('c')
        rawDocument.containsKey('d')
    }

    def 'containsKey should not find a non-existing key'() {
        expect:
        !rawDocument.containsKey('e')
        !rawDocument.containsKey('x')
        !rawDocument.containsKey('y')
    }

    def 'containValue should find an existing value'() {
        expect:
        rawDocument.containsValue(document.get('a'))
        rawDocument.containsValue(document.get('b'))
        rawDocument.containsValue(document.get('c'))
        rawDocument.containsValue(document.get('d'))
    }

    def 'containValue should not find a non-existing value'() {
        expect:
        !rawDocument.containsValue(new BsonInt32(3))
        !rawDocument.containsValue(new BsonDocument('e', BsonBoolean.FALSE))
        !rawDocument.containsValue(new BsonArray(asList(new BsonInt32(2), new BsonInt32(4))))
    }

    def 'isEmpty should return false when the document is not empty'() {
        expect:
        !rawDocument.isEmpty()
    }

    def 'isEmpty should return true when the document is empty'() {
        expect:
        emptyRawDocument.isEmpty()
    }

    def 'should get correct size'() {
        expect:
        emptyRawDocument.size() == 0
        rawDocument.size() == 4
    }

    def 'should get correct key set'() {
        expect:
        emptyRawDocument.keySet().isEmpty()
        rawDocument.keySet() == ['a', 'b', 'c', 'd'] as Set
    }

    def 'should get correct values set'() {
        expect:
        emptyRawDocument.values().isEmpty()
        rawDocument.values() as Set == [document.get('a'), document.get('b'), document.get('c'), document.get('d')] as Set
    }

    def 'should get correct entry set'() {
        expect:
        emptyRawDocument.entrySet().isEmpty()
        rawDocument.entrySet() == [new TestEntry('a', document.get('a')),
                                   new TestEntry('b', document.get('b')),
                                   new TestEntry('c', document.get('c')),
                                   new TestEntry('d', document.get('d'))] as Set
    }

    def 'all write methods should throw UnsupportedOperationException'() {
        when:
        rawDocument.clear()

        then:
        thrown(UnsupportedOperationException)

        when:
        rawDocument.put('x', BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        rawDocument.append('x', BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        rawDocument.putAll(new BsonDocument('x', BsonNull.VALUE))

        then:
        thrown(UnsupportedOperationException)

        when:
        rawDocument.remove(BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should decode'() {
        rawDocument.decode(new BsonDocumentCodec()) == document
    }

    class TestEntry implements Map.Entry<String, BsonValue> {

        private final String key;
        private BsonValue value

        TestEntry (String key, BsonValue value) {
            this.key = key
            this.value = value
        }

        @Override
        String getKey() {
            key
        }

        @Override
        BsonValue getValue() {
            value
        }

        @Override
        BsonValue setValue(final BsonValue value) {
            this.value = value
        }
    }
}