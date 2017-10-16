/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

class ByteBufPayloadBsonDocumentSpecification extends Specification {

    def commandDocument = BsonDocument.parse('{a: 1, b: 2, c: {x: true}, d: [{y: false}, 1]}')
    def payloadName = 'documents'
    def payloadDocuments = ['{a: "Alpha"}', '{b: "Bravo"}', '{c: "Charlie"}', '{d: "Delta"}'].collect { BsonDocument.parse(it) }
    def equivalentDocument =  BsonDocument.parse('''{a: 1, b: 2, c: {x: true}, d: [{y: false}, 1], documents: [
        {a: "Alpha"}, {b: "Bravo"}, {c: "Charlie"}, {d: "Delta"}]}''')

    List<ByteBuf> documentByteBufs
    List<ByteBufBsonDocument> byteBufBsonDocuments

    ByteBufPayloadBsonDocument byteBufPayloadDocument

    def setup() {
        documentByteBufs = [commandDocument, *payloadDocuments].collect { encode(it) }
        byteBufBsonDocuments = documentByteBufs.collect { new ByteBufBsonDocument(it) }
        byteBufPayloadDocument = new ByteBufPayloadBsonDocument(byteBufBsonDocuments.head(), payloadName, byteBufBsonDocuments.tail())
    }

    def 'get should get the value of the given key'() {
        expect:
        byteBufPayloadDocument.get('z') == null
        byteBufPayloadDocument.get('a') == new BsonInt32(1)
        byteBufPayloadDocument.get('b') == new BsonInt32(2)
        byteBufPayloadDocument.get(payloadName) == new BsonArray(byteBufBsonDocuments.tail())
    }

    def 'get should throw if the key is null'() {
        when:
        byteBufPayloadDocument.get(null)

        then:
        thrown(IllegalArgumentException)
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'containKey should throw if the key name is null'() {
        when:
        byteBufPayloadDocument.containsKey(null)

        then:
        thrown(IllegalArgumentException)
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'containsKey should find an existing key'() {
        expect:
        byteBufPayloadDocument.containsKey('a')
        byteBufPayloadDocument.containsKey('b')
        byteBufPayloadDocument.containsKey('c')
        byteBufPayloadDocument.containsKey('d')
        byteBufPayloadDocument.containsKey('documents')
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'containsKey should not find a non-existing key'() {
        expect:
        !byteBufPayloadDocument.containsKey('e')
        !byteBufPayloadDocument.containsKey('x')
        !byteBufPayloadDocument.containsKey('y')
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'containValue should find an existing value'() {
        expect:
        byteBufPayloadDocument.containsValue(commandDocument.get('a'))
        byteBufPayloadDocument.containsValue(commandDocument.get('b'))
        byteBufPayloadDocument.containsValue(commandDocument.get('c'))
        byteBufPayloadDocument.containsValue(commandDocument.get('d'))
        byteBufPayloadDocument.containsValue(new BsonArray(payloadDocuments))
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'containValue should not find a non-existing value'() {
        expect:
        !byteBufPayloadDocument.containsValue(new BsonInt32(3))
        !byteBufPayloadDocument.containsValue(new BsonDocument('e', BsonBoolean.FALSE))
        !byteBufPayloadDocument.containsValue(new BsonArray(asList(new BsonInt32(2), new BsonInt32(4))))
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'isEmpty should return false when the document is not empty'() {
        expect:
        !byteBufPayloadDocument.isEmpty()
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'should get correct size'() {
        expect:
        byteBufPayloadDocument.size() == 5
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'should get correct key set'() {
        expect:
        byteBufPayloadDocument.keySet() == ['a', 'b', 'c', 'd', 'documents'] as Set
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'should get correct values set'() {
        expect:
        byteBufPayloadDocument.values() as Set == [commandDocument.get('a'), commandDocument.get('b'), commandDocument.get('c'),
                                                   commandDocument.get('d'), new BsonArray(payloadDocuments)] as Set
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'should get correct entry set'() {
        expect:
        byteBufPayloadDocument.entrySet() == [new TestEntry('a', commandDocument.get('a')),
                                              new TestEntry('b', commandDocument.get('b')),
                                              new TestEntry('c', commandDocument.get('c')),
                                              new TestEntry('d', commandDocument.get('d')),
                                              new TestEntry('documents', new BsonArray(payloadDocuments))] as Set
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'all write methods should throw UnsupportedOperationException'() {
        when:
        byteBufPayloadDocument.clear()

        then:
        thrown(UnsupportedOperationException)

        when:
        byteBufPayloadDocument.put('x', BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        byteBufPayloadDocument.append('x', BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        byteBufPayloadDocument.putAll(new BsonDocument('x', BsonNull.VALUE))

        then:
        thrown(UnsupportedOperationException)

        when:
        byteBufPayloadDocument.remove(BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should get first key'() {
        expect:
        byteBufPayloadDocument.getFirstKey() == commandDocument.keySet().iterator().next()
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'hashCode should equal hash code of identical BsonDocument'() {
        expect:
        byteBufPayloadDocument.hashCode() == equivalentDocument.hashCode()
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'equals should equal identical BsonDocument'() {
        expect:
        areEqual(byteBufPayloadDocument, equivalentDocument)
        !areEqual(commandDocument, byteBufPayloadDocument)
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'clone should make a deep copy'() {
        when:
        BsonDocument cloned = byteBufPayloadDocument.clone()

        then:
        cloned == byteBufPayloadDocument
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'should serialize and deserialize'() {
        given:
        def baos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(baos)

        when:
        oos.writeObject(byteBufPayloadDocument)
        def bais = new ByteArrayInputStream(baos.toByteArray())
        def ois = new ObjectInputStream(bais)
        def deserializedDocument = ois.readObject()

        then:
        byteBufPayloadDocument == deserializedDocument
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'toJson should return equivalent'() {
        expect:
        equivalentDocument.toJson() == byteBufPayloadDocument.toJson()
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'toJson should be callable multiple times'() {
        expect:
        byteBufPayloadDocument.toJson()
        byteBufPayloadDocument.toJson()
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'size should be callable multiple times'() {
        expect:
        byteBufPayloadDocument.size()
        byteBufPayloadDocument.size()
        documentByteBufs*.referenceCount.each { assert it == 1 }
    }

    def 'toJson should respect JsonWriteSettings'() {
        given:
        def settings = new JsonWriterSettings(JsonMode.SHELL)

        expect:
        equivalentDocument.toJson(settings) == byteBufPayloadDocument.toJson(settings)
    }

    def 'toJson should return equivalent when a ByteBufPayloadBsonDocument is nested in a BsonDocument'() {
        given:
        def topLevel = new BsonDocument('nested', byteBufPayloadDocument)

        expect:
        new BsonDocument('nested', equivalentDocument).toJson() == topLevel.toJson()
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

    private static ByteBuf encode(BsonDocument document) {
        def buffer = new BasicOutputBuffer()
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build())
        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        buffer.pipe(baos)
        new ByteBufNIO(ByteBuffer.wrap(baos.toByteArray()))
    }

}
