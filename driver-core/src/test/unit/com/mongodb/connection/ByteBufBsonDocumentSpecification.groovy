/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.connection

import org.bson.BsonArray
import org.bson.BsonBinaryWriter
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonNull
import org.bson.BsonValue
import org.bson.ByteBuf
import org.bson.ByteBufNIO
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.EncoderContext
import org.bson.io.BasicOutputBuffer
import org.bson.json.JsonMode
import org.bson.json.JsonWriterSettings
import spock.lang.Specification

import java.nio.ByteBuffer

import static java.util.Arrays.asList
import static util.GroovyHelpers.areEqual

class ByteBufBsonDocumentSpecification extends Specification {
    def emptyDocumentByteBuf = new ByteBufNIO(ByteBuffer.wrap([5, 0, 0, 0, 0] as byte[]))
    ByteBuf documentByteBuf
    ByteBufBsonDocument emptyRawDocument = new ByteBufBsonDocument(emptyDocumentByteBuf);
    def document = new BsonDocument()
            .append('a', new BsonInt32(1))
            .append('b', new BsonInt32(2))
            .append('c', new BsonDocument('x', BsonBoolean.TRUE))
            .append('d', new BsonArray(asList(new BsonDocument('y', BsonBoolean.FALSE), new BsonInt32(1))))

    ByteBufBsonDocument rawDocument

    def setup() {
        def buffer = new BasicOutputBuffer()
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        buffer.pipe(baos)
        documentByteBuf = new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray()))
        rawDocument = new ByteBufBsonDocument(documentByteBuf);
    }

    def 'get should get the value of the given key'() {
        expect:
        emptyRawDocument.get('a') == null
        rawDocument.get('z') == null
        rawDocument.get('a') == new BsonInt32(1)
        rawDocument.get('b') == new BsonInt32(2)

    }

    def 'get should throw if the key is null'() {
        when:
        rawDocument.get(null)

        then:
        thrown(IllegalArgumentException)
        documentByteBuf.referenceCount == 1
    }

    def 'containKey should throw if the key name is null'() {
        when:
        rawDocument.containsKey(null)

        then:
        thrown(IllegalArgumentException)
        documentByteBuf.referenceCount == 1
    }

    def 'containsKey should find an existing key'() {
        expect:
        rawDocument.containsKey('a')
        rawDocument.containsKey('b')
        rawDocument.containsKey('c')
        rawDocument.containsKey('d')
        documentByteBuf.referenceCount == 1
    }

    def 'containsKey should not find a non-existing key'() {
        expect:
        !rawDocument.containsKey('e')
        !rawDocument.containsKey('x')
        !rawDocument.containsKey('y')
        documentByteBuf.referenceCount == 1
    }

    def 'containValue should find an existing value'() {
        expect:
        rawDocument.containsValue(document.get('a'))
        rawDocument.containsValue(document.get('b'))
        rawDocument.containsValue(document.get('c'))
        rawDocument.containsValue(document.get('d'))
        documentByteBuf.referenceCount == 1
    }

    def 'containValue should not find a non-existing value'() {
        expect:
        !rawDocument.containsValue(new BsonInt32(3))
        !rawDocument.containsValue(new BsonDocument('e', BsonBoolean.FALSE))
        !rawDocument.containsValue(new BsonArray(asList(new BsonInt32(2), new BsonInt32(4))))
        documentByteBuf.referenceCount == 1
    }

    def 'isEmpty should return false when the document is not empty'() {
        expect:
        !rawDocument.isEmpty()
        documentByteBuf.referenceCount == 1
    }

    def 'isEmpty should return true when the document is empty'() {
        expect:
        emptyRawDocument.isEmpty()
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should get correct size'() {
        expect:
        emptyRawDocument.size() == 0
        rawDocument.size() == 4
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should get correct key set'() {
        expect:
        emptyRawDocument.keySet().isEmpty()
        rawDocument.keySet() == ['a', 'b', 'c', 'd'] as Set
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should get correct values set'() {
        expect:
        emptyRawDocument.values().isEmpty()
        rawDocument.values() as Set == [document.get('a'), document.get('b'), document.get('c'), document.get('d')] as Set
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should get correct entry set'() {
        expect:
        emptyRawDocument.entrySet().isEmpty()
        rawDocument.entrySet() == [new TestEntry('a', document.get('a')),
                                   new TestEntry('b', document.get('b')),
                                   new TestEntry('c', document.get('c')),
                                   new TestEntry('d', document.get('d'))] as Set
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
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

    def 'should get first key'() {
        expect:
        rawDocument.getFirstKey() == document.keySet().iterator().next()
        emptyRawDocument.getFirstKey() == null
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'hashCode should equal hash code of identical BsonDocument'() {
        expect:
        rawDocument.hashCode() == document.hashCode()
        documentByteBuf.referenceCount == 1
    }

    def 'equals should equal identical BsonDocument'() {
        expect:
        areEqual(rawDocument, document)
        areEqual(document, rawDocument)
        areEqual(rawDocument, rawDocument)
        !areEqual(rawDocument, emptyRawDocument)
        documentByteBuf.referenceCount == 1
    }

    def 'clone should make a deep copy'() {
        when:
        BsonDocument cloned = rawDocument.clone()

        then:
        cloned == rawDocument
        documentByteBuf.referenceCount == 1
    }

    def 'should serialize and deserialize'() {
        given:
        def baos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(baos)

        when:
        oos.writeObject(rawDocument)
        def bais = new ByteArrayInputStream(baos.toByteArray())
        def ois = new ObjectInputStream(bais)
        def deserializedDocument = ois.readObject()

        then:
        rawDocument == deserializedDocument
        documentByteBuf.referenceCount == 1
    }

    def 'toJson should return equivalent'() {
        expect:
        document.toJson() == rawDocument.toJson()
        documentByteBuf.referenceCount == 1
    }

    def 'toJson should be callable multiple times'() {
        expect:
        rawDocument.toJson()
        rawDocument.toJson()
        documentByteBuf.referenceCount == 1
    }

    def 'size should be callable multiple times'() {
        expect:
        rawDocument.size()
        rawDocument.size()
        documentByteBuf.referenceCount == 1
    }

    def 'toJson should respect JsonWriteSettings'() {
        given:
        def settings = new JsonWriterSettings(JsonMode.SHELL);

        expect:
        document.toJson(settings) == rawDocument.toJson(settings)
    }

    def 'toJson should return equivalent when a ByteBufBsonDocument is nested in a BsonDocument'() {
        given:
        def topLevel = new BsonDocument('nested', rawDocument)

        expect:
        new BsonDocument('nested', document).toJson() == topLevel.toJson()
    }

    class TestEntry implements Map.Entry<String, BsonValue> {

        private final String key;
        private BsonValue value

        TestEntry(String key, BsonValue value) {
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
