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

package org.bson

import org.bson.io.BasicOutputBuffer
import org.bson.types.Binary
import org.bson.types.BsonDocument
import org.junit.Test
import spock.lang.Specification


class BsonWriterSpecification extends Specification {

    def shouldThrowExceptionForBooleanWhenWritingBeforeStartingDocument() {
        when:
        writer.writeBoolean("b1", true);

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowExceptionForArrayWhenWritingBeforeStartingDocument() {
        when:
        writer.writeStartArray();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowExceptionForNullWhenWritingBeforeStartingDocument() {
        when:
        writer.writeNull();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowExceptionForStringWhenStateIsValue() {
        when:
        writer.writeStartDocument();
        writer.writeString("SomeString");

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowExceptionWhenEndingAnArrayWhenStateIsValue() {
        when:
        writer.writeStartDocument();
        writer.writeEndArray();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowExceptionWhenWritingASecondName() {
        when:
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeName("i2");

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowExceptionWhenEndingADocumentBeforeValueIsWritten() {
        when:
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeEndDocument();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnExceptionWhenTryingToWriteASecondValue() {
        when:
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeString("i2");

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnExceptionWhenTryingToWriteJavaScript() {
        when:
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeJavaScript("var i");

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnExceptionWhenWritingANameInAnArray() {
        when:
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeName("i3");

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnExceptionWhenEndingDocumentInTheMiddleOfWritingAnArray() {
        when:
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeEndDocument();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnExceptionWhenEndingAnArrayInASubDocument() {
        when:
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeStartDocument();
        writer.writeEndArray();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnExceptionWhenWritingANameInAnArrayEvenWhenSubDocumentExistsInArray() {
        when:
        //Does this test even make sense?
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeStartDocument();
        writer.writeEndDocument();
        writer.writeName("i3");

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowExceptionWhenWritingObjectsIntoNestedArrays() {
        when:
//This test seem redundant?
        writer.writeStartDocument();
        writer.writeName("f1");
        writer.writeDouble(100);
        writer.writeStartArray("f2");
        writer.writeStartArray();
        writer.writeStartArray();
        writer.writeStartArray();
        writer.writeInt64("i4", 10);

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnExceptionWhenAttemptingToEndAnArrayThatWasNotStarted() {
        when:
        writer.writeStartDocument();
        writer.writeStartArray("f2");
        writer.writeEndArray();
        writer.writeEndArray();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope1() {
        when:
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeBoolean("b4", true);

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    @Test(expected = BsonInvalidOperationException.class)
    def shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope2() {
        when:
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeBinaryData(new Binary([0, 0, 1, 0] as byte[]));

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope3() {
        when:
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeStartArray();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnErrorIfTryingToWriteNamesIntoAJavascriptScope4() {
        when:
        //do we really need to test every type written after writeJavaScriptWithScope?
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeEndDocument();

        then:
        thrown(BsonInvalidOperationException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowAnErrorIfKeyContainsNullCharacter() {
        when:
        writer.writeStartDocument();
        writer.writeBoolean("h\u0000i", true);


        then:
        thrown(BSONException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldNotThrowAnErrorIfValueContainsNullCharacter() {
        when:
        writer.writeStartDocument();
        writer.writeString("x", "h\u0000i");

        then:
        true

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldNotThrowAnExceptionIfCorrectlyStartingAndEndingDocumentsAndSubDocuments() {
        when:
        writer.writeStartDocument();
        writer.writeJavaScriptWithScope("js1", "var i = 1");

        writer.writeStartDocument();
        writer.writeEndDocument();

        writer.writeEndDocument();

        then:
        true

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), true), new BsonDocumentWriter(new BsonDocument())]
    }

    def shouldThrowOnInvalidFieldName() {
        given:
        writer.writeStartDocument();
        writer.writeString('good', 'string')

        when:
        writer.writeString('bad', 'string')

        then:
        thrown(IllegalArgumentException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator('bad'))]
    }

    def shouldThrowOnInvalidFieldNameNestedInDocument() {
        given:
        writer.writeStartDocument()
        writer.writeName("doc")
        writer.writeStartDocument()
        writer.writeString('good', 'string')
        writer.writeString('bad', 'string')

        when:
        writer.writeString('bad-child', 'string')

        then:
        thrown(IllegalArgumentException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator('bad'))]
    }

    def shouldThrowOnInvalidFieldNameNestedInDocumentInArray() {
        given:
        writer.writeStartDocument()
        writer.writeName("doc")
        writer.writeStartArray()
        writer.writeStartDocument()
        writer.writeString('good', 'string')
        writer.writeString('bad', 'string')

        when:
        writer.writeString('bad-child', 'string')

        then:
        thrown(IllegalArgumentException)

        where:
        writer << [new BsonBinaryWriter(new BasicOutputBuffer(), new TestFieldNameValidator('bad'))]
    }


    class TestFieldNameValidator implements FieldNameValidator {
        private final String badFieldName;


        TestFieldNameValidator(final String badFieldName) {
            this.badFieldName = badFieldName
        }

        @Override
        boolean validate(final String fieldName) {
            return fieldName != badFieldName
        }

        @Override
        FieldNameValidator getValidatorForField(final String fieldName) {
            return new TestFieldNameValidator(badFieldName + '-child')
        }
    }

}

