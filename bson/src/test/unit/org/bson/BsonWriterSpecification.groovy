/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

package org.bson

import org.bson.io.BasicOutputBuffer
import spock.lang.Specification

class BsonWriterSpecification extends Specification {

    def 'shouldThrowExceptionForBooleanWhenWritingBeforeStartingDocument'() {
        when:
        writer.writeBoolean('b1', true)

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowExceptionForArrayWhenWritingBeforeStartingDocument'() {
        when:
        writer.writeStartArray()

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowExceptionForNullWhenWritingBeforeStartingDocument'() {
        when:
        writer.writeNull()

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowExceptionForStringWhenStateIsValue'() {
        when:
        writer.writeStartDocument()
        writer.writeString('SomeString')

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowExceptionWhenEndingAnArrayWhenStateIsValue'() {
        when:
        writer.writeStartDocument()
        writer.writeEndArray()

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowExceptionWhenWritingASecondName'() {
        when:
        writer.writeStartDocument()
        writer.writeName('f1')
        writer.writeName('i2')

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowExceptionWhenEndingADocumentBeforeValueIsWritten'() {
        when:
        writer.writeStartDocument()
        writer.writeName('f1')
        writer.writeEndDocument()

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnExceptionWhenTryingToWriteASecondValue'() {
        when:
        writer.writeStartDocument()
        writer.writeName('f1')
        writer.writeDouble(100)
        writer.writeString('i2')

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnExceptionWhenTryingToWriteJavaScript'() {
        when:
        writer.writeStartDocument()
        writer.writeName('f1')
        writer.writeDouble(100)
        writer.writeJavaScript('var i')

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnExceptionWhenWritingANameInAnArray'() {
        when:
        writer.writeStartDocument()
        writer.writeName('f1')
        writer.writeDouble(100)
        writer.writeStartArray('f2')
        writer.writeName('i3')

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnExceptionWhenEndingDocumentInTheMiddleOfWritingAnArray'() {
        when:
        writer.writeStartDocument()
        writer.writeName('f1')
        writer.writeDouble(100)
        writer.writeStartArray('f2')
        writer.writeEndDocument()

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnExceptionWhenEndingAnArrayInASubDocument'() {
        when:
        writer.with {
            writeStartDocument()
            writeName('f1')
            writeDouble(100)
            writeStartArray('f2')
            writeStartDocument()
            writeEndArray()
        }

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnExceptionWhenWritingANameInAnArrayEvenWhenSubDocumentExistsInArray'() {
        when:
        //Does this test even make sense?
        writer.with {
            writeStartDocument()
            writeName('f1')
            writeDouble(100)
            writeStartArray('f2')
            writeStartDocument()
            writeEndDocument()
            writeName('i3')
        }

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowExceptionWhenWritingObjectsIntoNestedArrays'() {
        when:
//This test seem redundant?
        writer.with {
            writeStartDocument()
            writeName('f1')
            writeDouble(100)
            writeStartArray('f2')
            writeStartArray()
            writeStartArray()
            writeStartArray()
            writeInt64('i4', 10)
        }
        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnExceptionWhenAttemptingToEndAnArrayThatWasNotStarted'() {
        when:
        writer.with {
            writeStartDocument()
            writeStartArray('f2')
            writeEndArray()
            writeEndArray()
        }
        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope1'() {
        when:
        writer.writeStartDocument()
        writer.writeJavaScriptWithScope('js1', 'var i = 1')

        writer.writeBoolean('b4', true)

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope2'() {
        when:
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument()
        writer.writeJavaScriptWithScope('js1', 'var i = 1')

        writer.writeBinaryData(new BsonBinary([0, 0, 1, 0] as byte[]))

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope3'() {
        when:
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument()
        writer.writeJavaScriptWithScope('js1', 'var i = 1')

        writer.writeStartArray()

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope4'() {
        when:
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument()
        writer.writeJavaScriptWithScope('js1', 'var i = 1')

        writer.writeEndDocument()

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowAnErrorIfKeyContainsNullCharacter'() {
        when:
        writer.writeStartDocument()
        writer.writeBoolean('h\u0000i', true)


        then:
        thrown(BSONException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer())]
    }

    def 'shouldNotThrowAnErrorIfValueContainsNullCharacter'() {
        when:
        writer.writeStartDocument()
        writer.writeString('x', 'h\u0000i')

        then:
        true

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldNotThrowAnExceptionIfCorrectlyStartingAndEndingDocumentsAndSubDocuments'() {
        when:
        writer.writeStartDocument()
        writer.writeJavaScriptWithScope('js1', 'var i = 1')

        writer.writeStartDocument()
        writer.writeEndDocument()

        writer.writeEndDocument()

        then:
        true

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer()), new BsonDocumentWriter(new BsonDocument())]
    }

    def 'shouldThrowOnInvalidFieldName'() {
        given:
        writer.writeStartDocument()
        writer.writeString('good', 'string')

        when:
        writer.writeString('bad', 'string')

        then:
        thrown(IllegalArgumentException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator('bad'))]
    }

    def 'shouldThrowOnInvalidFieldNameNestedInDocument'() {
        given:
        writer.with {
            writeStartDocument()
            writeName('doc')
            writeStartDocument()
            writeString('good', 'string')
            writeString('bad', 'string')
        }
        when:
        writer.writeString('bad-child', 'string')

        then:
        thrown(IllegalArgumentException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator('bad'))]
    }

    def 'shouldThrowOnInvalidFieldNameNestedInDocumentInArray'() {
        given:
        writer.with {
            writeStartDocument()
            writeName('doc')
            writeStartArray()
            writeStartDocument()
            writeString('good', 'string')
            writeString('bad', 'string')
        }
        when:
        writer.writeString('bad-child', 'string')

        then:
        def e = thrown(IllegalArgumentException)
        e.getMessage() == 'testFieldNameValidator error'

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator('bad'))]
    }

    class TestFieldNameValidator implements FieldNameValidator {
        private final String badFieldName

        TestFieldNameValidator(final String badFieldName) {
            this.badFieldName = badFieldName
        }

        @Override
        boolean validate(final String fieldName) {
            fieldName != badFieldName
        }

        @Override
        String getValidationErrorMessage(final String fieldName) {
            'testFieldNameValidator error'
        }

        @Override
        FieldNameValidator getValidatorForField(final String fieldName) {
            new TestFieldNameValidator(badFieldName + '-child')
        }
    }

}

