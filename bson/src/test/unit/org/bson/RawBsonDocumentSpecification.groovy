/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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
import org.bson.codecs.DecoderContext
import org.bson.codecs.DocumentCodec
import org.bson.codecs.EncoderContext
import org.bson.codecs.RawBsonDocumentCodec
import org.bson.io.BasicOutputBuffer
import org.bson.json.JsonMode
import org.bson.json.JsonReader
import org.bson.json.JsonWriter
import org.bson.json.JsonWriterSettings
import spock.lang.Specification

import java.nio.ByteOrder

import static java.util.Arrays.asList
import static util.GroovyHelpers.areEqual

class RawBsonDocumentSpecification extends Specification {

    static emptyDocument = new BsonDocument()
    static emptyRawDocument = new RawBsonDocument(emptyDocument, new BsonDocumentCodec());
    static document = new BsonDocument()
            .append('a', new BsonInt32(1))
            .append('b', new BsonInt32(2))
            .append('c', new BsonDocument('x', BsonBoolean.TRUE))
            .append('d', new BsonArray(asList(new BsonDocument('y', BsonBoolean.FALSE), new BsonInt32(1))))

    def 'constructors should throw if parameters are invalid'() {
        when:
        new RawBsonDocument(null)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonDocument(null, 0, 5)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonDocument(new byte[5], -1, 5)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonDocument(new byte[5], 5, 5)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonDocument(new byte[5], 0, 0)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonDocument(new byte[10], 6, 5)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonDocument(null, new DocumentCodec())

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonDocument(new Document(), null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'byteBuffer should contain the correct bytes'() {
        when:
        def byteBuf = rawDocument.getByteBuffer()

        then:
        rawDocument == document
        byteBuf.asNIO().order() == ByteOrder.LITTLE_ENDIAN
        byteBuf.remaining() == 58

        when:
        def actualBytes = new byte[58]
        byteBuf.get(actualBytes)

        then:
        actualBytes == getBytesFromDocument()

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'parse should through if parameter is invalid'() {
        when:
        RawBsonDocument.parse(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should parse json'() {
        expect:
        RawBsonDocument.parse('{a : 1}') == new BsonDocument('a', new BsonInt32(1))
    }

    def 'containKey should throw if the key name is null'() {
        when:
        rawDocument.containsKey(null)

        then:
        thrown(IllegalArgumentException)

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'containsKey should find an existing key'() {
        expect:
        rawDocument.containsKey('a')
        rawDocument.containsKey('b')
        rawDocument.containsKey('c')
        rawDocument.containsKey('d')

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'containsKey should not find a non-existing key'() {
        expect:
        !rawDocument.containsKey('e')
        !rawDocument.containsKey('x')
        !rawDocument.containsKey('y')
        rawDocument.get('e') == null
        rawDocument.get('x') == null
        rawDocument.get('y') == null

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'containValue should find an existing value'() {
        expect:
        rawDocument.containsValue(document.get('a'))
        rawDocument.containsValue(document.get('b'))
        rawDocument.containsValue(document.get('c'))
        rawDocument.containsValue(document.get('d'))

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'containValue should not find a non-existing value'() {
        expect:
        !rawDocument.containsValue(new BsonInt32(3))
        !rawDocument.containsValue(new BsonDocument('e', BsonBoolean.FALSE))
        !rawDocument.containsValue(new BsonArray(asList(new BsonInt32(2), new BsonInt32(4))))

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'isEmpty should return false when the document is not empty'() {
        expect:
        !rawDocument.isEmpty()

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'isEmpty should return true when the document is empty'() {
        expect:
        emptyRawDocument.isEmpty()
    }

    def 'should get correct size when the document is empty'() {
        expect:
        emptyRawDocument.size() == 0
    }

    def 'should get correct key set when the document is empty'() {
        expect:
        emptyRawDocument.keySet().isEmpty()
    }

    def 'should get correct values set when the document is empty'() {
        expect:
        emptyRawDocument.values().isEmpty()
    }

    def 'should get correct entry set when the document is empty'() {
        expect:
        emptyRawDocument.entrySet().isEmpty()
    }

    def 'should get correct size'() {
        expect:
        createRawDocumenFromDocument().size() == 4

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'should get correct key set'() {
        expect:
        rawDocument.keySet() == ['a', 'b', 'c', 'd'] as Set

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'should get correct values set'() {
        expect:
        rawDocument.values() as Set == [document.get('a'), document.get('b'), document.get('c'), document.get('d')] as Set

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'should get correct entry set'() {
        expect:
        rawDocument.entrySet() == [new TestEntry('a', document.get('a')),
                                   new TestEntry('b', document.get('b')),
                                   new TestEntry('c', document.get('c')),
                                   new TestEntry('d', document.get('d'))] as Set

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'should get first key'() {
        expect:
        document.getFirstKey() == 'a'

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'getFirstKey should throw NoSuchElementException if the document is empty'() {
        when:
        emptyRawDocument.getFirstKey()

        then:
        thrown(NoSuchElementException)
    }

    def 'should create BsonReader'() {
        when:
        def reader = document.asBsonReader()

        then:
        new BsonDocumentCodec().decode(reader, DecoderContext.builder().build()) == document

        cleanup:
        reader.close()
    }

    def 'toJson should return equivalent JSON'() {
        expect:
        new RawBsonDocumentCodec().decode(new JsonReader(rawDocument.toJson()), DecoderContext.builder().build()) == document

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'toJson should respect default JsonWriterSettings'() {
        given:
        def writer = new StringWriter();

        when:
        new BsonDocumentCodec().encode(new JsonWriter(writer), document, EncoderContext.builder().build());

        then:
        writer.toString() == rawDocument.toJson()

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'toJson should respect JsonWriterSettings'() {
        given:
        def jsonWriterSettings = new JsonWriterSettings(JsonMode.SHELL)
        def writer = new StringWriter();

        when:
        new RawBsonDocumentCodec().encode(new JsonWriter(writer, jsonWriterSettings), rawDocument, EncoderContext.builder().build());

        then:
        writer.toString() == rawDocument.toJson(jsonWriterSettings)

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'all write methods should throw UnsupportedOperationException'() {
        given:
        def rawDocument = createRawDocumenFromDocument()

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

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'hashCode should equal hash code of identical BsonDocument'() {
        expect:
        rawDocument.hashCode() == document.hashCode()

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'equals should equal identical BsonDocument'() {
        expect:
        areEqual(rawDocument, document)
        areEqual(document, rawDocument)
        areEqual(rawDocument, rawDocument)
        !areEqual(rawDocument, emptyRawDocument)

        where:
        rawDocument << createRawDocumentVariants()
    }

    def 'clone should make a deep copy'() {
        when:
        RawBsonDocument cloned = rawDocument.clone()

        then:
        !cloned.getByteBuffer().array().is(createRawDocumenFromDocument().getByteBuffer().array())
        cloned.getByteBuffer().remaining() == rawDocument.getByteBuffer().remaining()
        cloned == createRawDocumenFromDocument()

        where:
        rawDocument << [
                createRawDocumenFromDocument(),
                createRawDocumentFromByteArray(),
                createRawDocumentFromByteArrayOffsetLength()
        ]
    }

    def 'should serialize and deserialize'() {
        given:
        def baos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(baos)

        when:
        oos.writeObject(localRawDocument)
        def bais = new ByteArrayInputStream(baos.toByteArray())
        def ois = new ObjectInputStream(bais)
        def deserializedDocument = ois.readObject()

        then:
        document == deserializedDocument

        where:
        localRawDocument << createRawDocumentVariants()
    }

    private static List<RawBsonDocument> createRawDocumentVariants() {
        [
                createRawDocumenFromDocument(),
                createRawDocumentFromByteArray(),
                createRawDocumentFromByteArrayOffsetLength()
        ]
    }

    private static RawBsonDocument createRawDocumenFromDocument() {
        new RawBsonDocument(document, new BsonDocumentCodec())
    }

    private static RawBsonDocument createRawDocumentFromByteArray() {
        byte[] strippedBytes = getBytesFromDocument()
        new RawBsonDocument(strippedBytes)
    }

    private static byte[] getBytesFromDocument() {
        def (int size, byte[] bytes) = getBytesFromOutputBuffer()
        def strippedBytes = new byte[size]
        System.arraycopy(bytes, 0, strippedBytes, 0, size)
        strippedBytes
    }

    private static List getBytesFromOutputBuffer() {
        def outputBuffer = new BasicOutputBuffer(1024)
        new BsonDocumentCodec().encode(new BsonBinaryWriter(outputBuffer), document, EncoderContext.builder().build())
        def bytes = outputBuffer.getInternalBuffer()
        [outputBuffer.position, bytes]
    }

    private static RawBsonDocument createRawDocumentFromByteArrayOffsetLength() {
        def (int size, byte[] bytes) = getBytesFromOutputBuffer()
        def unstrippedBytes = new byte[size + 2]
        System.arraycopy(bytes, 0, unstrippedBytes, 1, size)
        new RawBsonDocument(unstrippedBytes, 1, size)
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
