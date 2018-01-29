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
import org.bson.codecs.DecoderContext
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
    ByteBufBsonDocument emptyByteBufDocument = new ByteBufBsonDocument(emptyDocumentByteBuf)
    def document = new BsonDocument()
            .append('a', new BsonInt32(1))
            .append('b', new BsonInt32(2))
            .append('c', new BsonDocument('x', BsonBoolean.TRUE))
            .append('d', new BsonArray(asList(new BsonDocument('y', BsonBoolean.FALSE), new BsonInt32(1))))

    ByteBufBsonDocument byteBufDocument

    def setup() {
        def buffer = new BasicOutputBuffer()
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build())
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        buffer.pipe(baos)
        documentByteBuf = new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray()))
        byteBufDocument = new ByteBufBsonDocument(documentByteBuf)
    }

    def 'get should get the value of the given key'() {
        expect:
        emptyByteBufDocument.get('a') == null
        byteBufDocument.get('z') == null
        byteBufDocument.get('a') == new BsonInt32(1)
        byteBufDocument.get('b') == new BsonInt32(2)
    }

    def 'get should throw if the key is null'() {
        when:
        byteBufDocument.get(null)

        then:
        thrown(IllegalArgumentException)
        documentByteBuf.referenceCount == 1
    }

    def 'containKey should throw if the key name is null'() {
        when:
        byteBufDocument.containsKey(null)

        then:
        thrown(IllegalArgumentException)
        documentByteBuf.referenceCount == 1
    }

    def 'containsKey should find an existing key'() {
        expect:
        byteBufDocument.containsKey('a')
        byteBufDocument.containsKey('b')
        byteBufDocument.containsKey('c')
        byteBufDocument.containsKey('d')
        documentByteBuf.referenceCount == 1
    }

    def 'containsKey should not find a non-existing key'() {
        expect:
        !byteBufDocument.containsKey('e')
        !byteBufDocument.containsKey('x')
        !byteBufDocument.containsKey('y')
        documentByteBuf.referenceCount == 1
    }

    def 'containValue should find an existing value'() {
        expect:
        byteBufDocument.containsValue(document.get('a'))
        byteBufDocument.containsValue(document.get('b'))
        byteBufDocument.containsValue(document.get('c'))
        byteBufDocument.containsValue(document.get('d'))
        documentByteBuf.referenceCount == 1
    }

    def 'containValue should not find a non-existing value'() {
        expect:
        !byteBufDocument.containsValue(new BsonInt32(3))
        !byteBufDocument.containsValue(new BsonDocument('e', BsonBoolean.FALSE))
        !byteBufDocument.containsValue(new BsonArray(asList(new BsonInt32(2), new BsonInt32(4))))
        documentByteBuf.referenceCount == 1
    }

    def 'isEmpty should return false when the document is not empty'() {
        expect:
        !byteBufDocument.isEmpty()
        documentByteBuf.referenceCount == 1
    }

    def 'isEmpty should return true when the document is empty'() {
        expect:
        emptyByteBufDocument.isEmpty()
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should get correct size'() {
        expect:
        emptyByteBufDocument.size() == 0
        byteBufDocument.size() == 4
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should get correct key set'() {
        expect:
        emptyByteBufDocument.keySet().isEmpty()
        byteBufDocument.keySet() == ['a', 'b', 'c', 'd'] as Set
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should get correct values set'() {
        expect:
        emptyByteBufDocument.values().isEmpty()
        byteBufDocument.values() as Set == [document.get('a'), document.get('b'), document.get('c'), document.get('d')] as Set
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should get correct entry set'() {
        expect:
        emptyByteBufDocument.entrySet().isEmpty()
        byteBufDocument.entrySet() == [new TestEntry('a', document.get('a')),
                                       new TestEntry('b', document.get('b')),
                                       new TestEntry('c', document.get('c')),
                                       new TestEntry('d', document.get('d'))] as Set
        documentByteBuf.referenceCount == 1
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'all write methods should throw UnsupportedOperationException'() {
        when:
        byteBufDocument.clear()

        then:
        thrown(UnsupportedOperationException)

        when:
        byteBufDocument.put('x', BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        byteBufDocument.append('x', BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        byteBufDocument.putAll(new BsonDocument('x', BsonNull.VALUE))

        then:
        thrown(UnsupportedOperationException)

        when:
        byteBufDocument.remove(BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should get first key'() {
        expect:
        byteBufDocument.getFirstKey() == document.keySet().iterator().next()
        documentByteBuf.referenceCount == 1
    }

    def 'getFirstKey should throw NoSuchElementException if the document is empty'() {
        when:
        emptyByteBufDocument.getFirstKey()

        then:
        thrown(NoSuchElementException)
        emptyDocumentByteBuf.referenceCount == 1
    }

    def 'should create BsonReader'() {
        when:
        def reader = document.asBsonReader()

        then:
        new BsonDocumentCodec().decode(reader, DecoderContext.builder().build()) == document

        cleanup:
        reader.close()
    }

   def 'hashCode should equal hash code of identical BsonDocument'() {
        expect:
        byteBufDocument.hashCode() == document.hashCode()
        documentByteBuf.referenceCount == 1
    }

    def 'equals should equal identical BsonDocument'() {
        expect:
        areEqual(byteBufDocument, document)
        areEqual(document, byteBufDocument)
        areEqual(byteBufDocument, byteBufDocument)
        !areEqual(byteBufDocument, emptyByteBufDocument)
        documentByteBuf.referenceCount == 1
    }

    def 'clone should make a deep copy'() {
        when:
        BsonDocument cloned = byteBufDocument.clone()

        then:
        cloned == byteBufDocument
        documentByteBuf.referenceCount == 1
    }

    def 'should serialize and deserialize'() {
        given:
        def baos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(baos)

        when:
        oos.writeObject(byteBufDocument)
        def bais = new ByteArrayInputStream(baos.toByteArray())
        def ois = new ObjectInputStream(bais)
        def deserializedDocument = ois.readObject()

        then:
        byteBufDocument == deserializedDocument
        documentByteBuf.referenceCount == 1
    }

    def 'toJson should return equivalent'() {
        expect:
        document.toJson() == byteBufDocument.toJson()
        documentByteBuf.referenceCount == 1
    }

    def 'toJson should be callable multiple times'() {
        expect:
        byteBufDocument.toJson()
        byteBufDocument.toJson()
        documentByteBuf.referenceCount == 1
    }

    def 'size should be callable multiple times'() {
        expect:
        byteBufDocument.size()
        byteBufDocument.size()
        documentByteBuf.referenceCount == 1
    }

    def 'toJson should respect JsonWriteSettings'() {
        given:
        def settings = new JsonWriterSettings(JsonMode.SHELL)

        expect:
        document.toJson(settings) == byteBufDocument.toJson(settings)
    }

    def 'toJson should return equivalent when a ByteBufBsonDocument is nested in a BsonDocument'() {
        given:
        def topLevel = new BsonDocument('nested', byteBufDocument)

        expect:
        new BsonDocument('nested', document).toJson() == topLevel.toJson()
    }

    class TestEntry implements Map.Entry<String, BsonValue> {

        private final String key
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
