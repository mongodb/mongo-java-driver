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



package org.bson.types

import org.bson.BsonRegularExpression
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.bson.json.JsonParseException
import spock.lang.Specification

class DocumentSpecification extends Specification {

    def 'should return correct type for each typed method'() {
        given:
        Date date = new Date();
        ObjectId objectId = new ObjectId();

        when:
        Document doc = new Document()
                .append('int', 1).append('long', 2L).append('double', 3.0 as double).append('string', 'hi').append('boolean', true)
                .append('objectId', objectId).append('date', date);

        then:
        doc.getInteger('int') == 1
        doc.getInteger('intNoVal', 42) == 42
        doc.getLong('long') == 2L
        doc.getDouble('double') == 3.0d
        doc.getString('string') == 'hi'
        doc.getBoolean('boolean')
        doc.getBoolean('booleanNoVal', true)
        doc.getObjectId('objectId') == objectId
        doc.getDate('date') == date


        doc.get('objectId', ObjectId) == objectId
        doc.get('int', Integer) == 1
        doc.get('long', Long) == 2L
        doc.get('double', Double) == 3.0d
        doc.get('string', String) == 'hi'
        doc.get('boolean', Boolean)
        doc.get('date', Date) == date

        doc.get('noVal', 42L) == 42L
        doc.get('noVal', 3.1d) == 3.1d
        doc.get('noVal', 'defVal') == 'defVal'
        doc.get('noVal', true)
        doc.get('noVal', objectId) == objectId
        doc.get('noVal', date) == date
        doc.get('noVal', objectId) == objectId
    }

    def 'should return a list with elements of the specified class'() {
        when:
        Document doc = Document.parse("{x: 1, y: ['two', 'three'], z: [{a: 'one'}, {b:2}], w: {a: ['One', 'Two']}}")
                .append('numberList', Arrays.asList(10, 20.5d, 30L))
        List<String> defaultList = Arrays.asList('a', 'b', 'c')

        then:
        doc.getList('y', String).get(0) == 'two'
        doc.getList('y', String).get(1) == 'three'
        doc.getList('z', Document).get(0).getString('a') == 'one'
        doc.getList('z', Document).get(1).getInteger('b') == 2
        doc.get('w', Document).getList('a', String).get(0) == 'One'
        doc.get('w', Document).getList('a', String).get(1) == 'Two'
        doc.getList('invalidKey', Document, defaultList).get(0) == 'a'
        doc.getList('invalidKey', Document, defaultList).get(1) == 'b'
        doc.getList('invalidKey', Document, defaultList).get(2) == 'c'
        doc.getList('numberList', Number).get(0) == 10
        doc.getList('numberList', Number).get(1) == 20.5d
        doc.getList('numberList', Number).get(2) == 30L
    }

    def 'should return null list when key is not found'() {
        when:
        Document doc = Document.parse('{x: 1}')

        then:
        doc.getList('a', String) == null
    }

    def 'should return specified default value when key is not found'() {
        when:
        Document doc = Document.parse('{x: 1}')
        List<String> defaultList = Arrays.asList('a', 'b', 'c')

        then:
        doc.getList('a', String, defaultList) == defaultList
    }

    def 'should throw an exception when the list elements are not objects of the specified class'() {
        given:
        Document doc = Document.parse('{x: 1, y: [{a: 1}, {b: 2}], z: [1, 2]}')

        when:
        doc.getList('x', String)

        then:
        thrown(ClassCastException)

        when:
        doc.getList('y', String)

        then:
        thrown(ClassCastException)

        when:
        doc.getList('z', String)

        then:
        thrown(ClassCastException)
    }

    def 'should parse a valid JSON string to a Document'() {
        when:
        Document document = Document.parse("{ 'int' : 1, 'string' : 'abc' }");

        then:
        document != null;
        document.keySet().size() == 2;
        document.getInteger('int') == 1;
        document.getString('string') == 'abc';

        when:
        document = Document.parse("{ 'int' : 1, 'string' : 'abc' }", new DocumentCodec());

        then:
        document != null;
        document.keySet().size() == 2;
        document.getInteger('int') == 1;
        document.getString('string') == 'abc';
    }

    def 'test parse method with mode'() {
        when:
        Document document = Document.parse("{'regex' : /abc/im }");

        then:
        document != null;
        document.keySet().size() == 1;

        BsonRegularExpression regularExpression = (BsonRegularExpression) document.get('regex');
        regularExpression.options == 'im'
        regularExpression.pattern == 'abc'
    }

    def 'should throw an exception when parsing an invalid JSON String'() {
        when:
        Document.parse("{ 'int' : 1, 'string' : }");

        then:
        thrown(JsonParseException)
    }

    def 'should cast to correct type'() {
        given:
        Document document = new Document('str', 'a string')

        when:
        String s = document.get('str', String)

        then:
        s == document.get('str')
    }

    def 'should throw ClassCastException when value is the wrong type'() {
        given:
        Document document = new Document('int', 'not an int')

        when:
        document.get('int', Integer)

        then:
        thrown(ClassCastException)
    }
}
