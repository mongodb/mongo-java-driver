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

package org.bson.codecs.jackson

import org.bson.BsonArray
import org.bson.BsonBoolean
import org.bson.BsonDateTime
import org.bson.BsonDocument
import org.bson.BsonDocumentWriter
import org.bson.BsonDouble
import org.bson.BsonElement
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonJavaScript
import org.bson.BsonNull
import org.bson.BsonObjectId
import org.bson.BsonRegularExpression
import org.bson.BsonString
import org.bson.BsonTimestamp
import org.bson.types.ObjectId
import spock.lang.Specification
import java.util.regex.Pattern

class BsonGeneratorSpecification extends Specification {


    def 'should generate BSON of char'() {

        given:
        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Character>(writer)
        char[] content = ['a','b']

        when:
        generator.writeStartObject()
        generator.writeFieldName("Test")
        generator.writeRaw(content, 0, 2)
        generator.writeEndObject()
        generator.close()

        then:
        doc.getString("Test").getValue().toCharArray() == content

    }

    def 'should generate BSON of stackedObjects'() {

        given:
        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Character>(writer)

        when:
        generator.writeStartObject();
        generator.writeFieldName("Test");
        generator.writeStartObject();
        generator.writeFieldName("Int64");
        generator.writeNumber(5L);
        generator.writeFieldName("data1")
        generator.writeStartObject();
        generator.writeFieldName("Int32");
        generator.writeNumber(10);
        generator.writeEndObject();
        generator.writeFieldName("data3");
        generator.writeStartObject();
        generator.writeFieldName("String");
        generator.writeString("hello");
        generator.writeEndObject();
        generator.writeEndObject();
        generator.close();

        then:
        doc == new BsonDocument("Test", new BsonDocument([
                new BsonElement("Int64",new BsonInt64(5L)),
                new BsonElement("data1", new BsonDocument(
                        "Int32", new BsonInt32(10)
                )),
                new BsonElement("data3", new BsonDocument(
                        "String", new BsonString("hello")
                ))
            ])
        );
    }

    def 'should generate BSON of array'() {


        given:
        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Character>(writer)

        when:
        generator.writeStartObject()
        generator.writeFieldName("Int32")
        generator.writeNumber(5)
        generator.writeFieldName("Arr")
        generator.writeStartArray()
        generator.writeString("a")
        generator.writeString("b")
        generator.writeString("c")
        generator.writeEndArray()
        generator.writeFieldName("Int64")
        generator.writeNumber(10L)
        generator.writeFieldName("Arr2")
        generator.writeStartArray()
        generator.writeStartArray()
        generator.writeString("d")
        generator.writeString("e")
        generator.writeEndArray()
        generator.writeStartArray()
        generator.writeString("f")
        generator.writeEndArray()
        generator.writeEndArray()
        generator.writeFieldName("Arr3")
        generator.writeStartArray()
        generator.writeStartObject()
        generator.writeFieldName("Str")
        generator.writeString("Hello")
        generator.writeEndObject()
        generator.writeEndArray()
        generator.writeEndObject()
        generator.close()

        then:
        doc == new BsonDocument([
                new BsonElement("Int32", new BsonInt32(5)),
                new BsonElement("Arr", new BsonArray([
                        new BsonString("a"),
                        new BsonString("b"),
                        new BsonString("c")
                ])),
                new BsonElement("Int64", new BsonInt64(10L)),
                new BsonElement("Arr2", new BsonArray([
                        new BsonArray([
                                new BsonString("d"),
                                new BsonString("e")
                        ]),
                        new BsonArray([
                                new BsonString("f")
                        ])
                ])),
                new BsonElement("Arr3", new BsonArray([
                        new BsonDocument("Str",new BsonString("Hello"))
                ]))

        ])
    }

    def 'should generate BSON of bytes'() {

        given:
        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Character>(writer)
        byte[] content = [(byte)1 ,(byte)1]

        when:
        generator.writeStartObject()
        generator.writeFieldName("Test")
        generator.writeBinary(content)
        generator.writeEndObject()
        generator.close()

        then:
        doc.getBinary("Test").getData() == content
    }

    def 'should generate BSON of raw string'() {

        given:
        def doc = new BsonDocument();
        def writer = new BsonDocumentWriter(doc);
        def generator = new JacksonBsonGenerator<Character>(writer);
        def content = "ab";

        when:
        generator.writeStartObject();
        generator.writeFieldName("Test");
        generator.writeRaw(content);
        generator.writeEndObject();
        generator.close();

        then:
        doc.getBinary("Test").getData() == content.toCharArray();

    }

    def 'should generate BSON of UTF8 String'() {

        given:
        def doc = new BsonDocument();
        def writer = new BsonDocumentWriter(doc);
        def generator = new JacksonBsonGenerator<Character>(writer);
        def content = "a\u20AC\u00A2\u00A2bb";
        def key = "a\u20AC\u00A2\u00A2bb";

        when:
        generator.writeStartObject();
        generator.writeFieldName(key);
        generator.writeString(content);
        generator.writeEndObject();
        generator.close();

        then:
        doc.getString(key).getValue() == content;

    }

    def 'should generate BSON of Date'() {

        given:
        def doc = new BsonDocument();
        def writer = new BsonDocumentWriter(doc);
        def generator = new JacksonBsonGenerator<Character>(writer);
        def content = new Date();

        when:
        generator.writeStartObject();
        generator.writeFieldName("date");
        generator.writeDate(content);
        generator.writeEndObject();
        generator.close();

        then:
        doc.getDateTime("date") == new BsonDateTime(content.getTime());

    }

    def 'should generate BSON of int'() {

        given:
        def number = 10
        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Integer>(writer)

        when:
        generator.writeStartObject()
        generator.writeFieldName("int")
        generator.writeNumber(number)
        generator.writeEndObject()

        then:
        doc == new BsonDocument([
                new BsonElement("int",new BsonInt32(number))
        ])

    }

    def 'should generate BSON of ObjectId'() {

        given:
        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Integer>(writer)
        def content = new ObjectId()

        when:
        generator.writeStartObject()
        generator.writeFieldName("Test")
        generator.writeObjectId(content)
        generator.writeEndObject()

        then:
        doc == new BsonDocument([
                new BsonElement("Test", new BsonObjectId(content))
        ])

    }

    def 'should generate BSON of Pattern'() {

        given:

        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Integer>(writer)
        def content = Pattern.compile("a.*a", Pattern.CASE_INSENSITIVE | Pattern.DOTALL)


        when:
        generator.writeStartObject()
        generator.writeFieldName("Test")
        generator.writeRegex(content);
        generator.writeEndObject()

        then:
        doc.getRegularExpression("Test") == new BsonRegularExpression(content.pattern(), content.flags()+"");

    }

    def 'should generate BSON of javascript'() {

        given:

        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<BsonJavaScript>(writer)
        def content = new BsonJavaScript("var a = 'this is some js code'")


        when:
        generator.writeStartObject()
        generator.writeFieldName("Test")
        generator.writeJavascript(content);
        generator.writeEndObject()

        then:
        doc.get("Test") == content;

    }

    def 'should generate BSON of timestamp'() {

        given:

        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<BsonTimestamp>(writer)
        def content = new BsonTimestamp(10,100)


        when:
        generator.writeStartObject()
        generator.writeFieldName("Test")
        generator.writeTimestamp(content);
        generator.writeEndObject()

        then:
        doc.get("Test") == content;

    }

    def 'should generate BSON of BigInteger as string'() {

        given:

        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Integer>(writer)
        def content = new BigDecimal(0.3)


        when:
        generator.writeStartObject()
        generator.writeFieldName("Test")
        generator.writeNumber(content);
        generator.writeEndObject()

        then:
        def val = doc.getString("Test").getValue()
        Math.abs(Double.valueOf(val) - Double.valueOf(0.3)) < 0.0001

    }

    def 'should generate BSON of Biary Data'() {

        given:

        def doc = new BsonDocument()
        def writer = new BsonDocumentWriter(doc)
        def generator = new JacksonBsonGenerator<Integer>(writer)
        byte[] binary = [(byte)0x05, (byte)0xff, (byte)0xaf,
            (byte)0x30, 'A', 'B', 'C', (byte)0x13, (byte)0x80,
            (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff]


        when:
        generator.writeStartObject()
        generator.writeFieldName("Test")
        generator.writeBinary(binary)
        generator.writeEndObject()

        then:
        binary == doc.getBinary("Test").getData()

    }

    def 'should generate BSON of float'() {

        given:
        def number = 10.0f;
        def doc = new BsonDocument();
        def writer = new BsonDocumentWriter(doc);
        def generator = new JacksonBsonGenerator<Float>(writer);

        when:
        generator.writeStartObject();
        generator.writeFieldName("float");
        generator.writeNumber(number);
        generator.writeEndObject();

        then:
        doc == new BsonDocument([
                new BsonElement("float",new BsonDouble(number))
        ])

    }

    def 'should generate BSON of string'() {

        given:
        def s = "this is a string";
        def doc = new BsonDocument();
        def writer = new BsonDocumentWriter(doc);
        def generator = new JacksonBsonGenerator<String>(writer);

        when:
        generator.writeStartObject();
        generator.writeFieldName("string");
        generator.writeString(s);
        generator.writeEndObject();

        then:
        doc == new BsonDocument([
                new BsonElement("string",new BsonString(s))
        ])

    }

    def 'should generate BSON of long'() {

        given:
        def number = 10L;
        def doc = new BsonDocument();
        def writer = new BsonDocumentWriter(doc);
        def generator = new JacksonBsonGenerator<Integer>(writer);

        when:
        generator.writeStartObject();
        generator.writeFieldName("int");
        generator.writeNumber(number);
        generator.writeEndObject();

        then:
        doc == new BsonDocument([
                new BsonElement("int",new BsonInt64(number))
        ])

    }

    def 'should generate BSON of null'() {

        given:
        def doc = new BsonDocument();
        def writer = new BsonDocumentWriter(doc);
        def generator = new JacksonBsonGenerator<Integer>(writer);

        when:
        generator.writeStartObject();
        generator.writeFieldName("null");
        generator.writeNull();
        generator.writeEndObject();

        then:
        doc == new BsonDocument([
                new BsonElement("null",new BsonNull())
        ])

    }

    def 'should generate BSON of bool'() {

        given:
        def bool = true;
        def doc = new BsonDocument();
        def writer = new BsonDocumentWriter(doc);
        def generator = new JacksonBsonGenerator<Integer>(writer);

        when:
        generator.writeStartObject();
        generator.writeFieldName("bool");
        generator.writeBoolean(bool);
        generator.writeEndObject();

        then:
        doc == new BsonDocument([
                new BsonElement("bool",new BsonBoolean(bool))
        ])

    }
}
