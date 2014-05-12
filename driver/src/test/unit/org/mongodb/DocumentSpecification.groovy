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



package org.mongodb

import org.bson.types.ObjectId
import org.mongodb.json.JSONMode
import org.mongodb.json.JSONParseException
import org.mongodb.test.Worker
import spock.lang.Specification

import java.util.regex.Pattern

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
        doc.getInteger('int') == 1;
        doc.getLong('long') == 2L;
        doc.getDouble('double') == 3.0;
        doc.getString('string') == 'hi';
        doc.getBoolean('boolean');
        doc.getObjectId('objectId') == objectId;
        doc.getDate('date') == date;
        doc.get('objectId', ObjectId) == objectId;
    }

    def 'should convert valid JSON string to a Document'() {
        when:
        Document document = Document.valueOf("{ 'int' : 1, 'string' : 'abc' }");

        then:
        document != null;
        document.keySet().size() == 2;
        document.getInteger('int') == 1;
        document.getString('string') == 'abc';
    }

    def 'test value of method with mode'() {
        given:
        Pattern expectedPattern = Pattern.compile('abc', Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);

        when:
        Document document = Document.valueOf("{'regex' : /abc/im }", JSONMode.JAVASCRIPT);

        then:
        document != null;
        document.keySet().size() == 1;

        Pattern actualPattern = (Pattern) document.get('regex');
        actualPattern.flags() == expectedPattern.flags();
        actualPattern.pattern() == expectedPattern.pattern();
    }

    def 'should throw an exception when parsing an invalid JSON String'() {
        when:
        Document.valueOf("{ 'int' : 1, 'string' : }");

        then:
        thrown(JSONParseException)
    }

    def 'should produce nice JSON when calling toString'() {
        expect:
        new Document('int', 1).append('string', 'abc').toString() == '{ "int" : 1, "string" : "abc" }';
    }

    def 'should be copyable'() {
        given:
        Date date = new Date();
        ObjectId objectId = new ObjectId();
        Document doc = new Document()
                .append('int', 1).append('long', 2L).append('double', 3.0 as double).append('string', 'hi').append('boolean', true)
                .append('objectId', objectId).append('date', date).append('list', Arrays.asList(1, 2, 3, 4));

        when:
        Document clone = new Document(doc)

        then:
        doc == clone
    }

    def 'should provide deep copies'() {
        given:
        Date date = new Date();
        ObjectId objectId = new ObjectId();
        Document subDoc = ['a': 1, 'b': '2', 'c': [1, 2, 3, 4]] as Document
        Document doc = ['objectId': objectId, 'date': date, 'list': [1, 2, 3, 4], 'subDoc': subDoc] as Document
        Document clone = new Document(doc)

        when:
        clone.put('objectId', new ObjectId())
        clone.put('date', new Date())

        List<Integer> list = (List<Integer>) clone.get('list')
        list.set(0, 2)
        list.set(1, 3)
        list.set(2, 4)
        list.set(3, 5)

        Document subDocCopy = (Document) clone.get('subDoc')
        subDocCopy.put('a', 2)
        subDocCopy.put('b', '3')
        subDocCopy.put('c', [2, 3, 4, 5])

        then:
        doc.getObjectId('objectId') == objectId
        doc.getDate('date') == date
        doc.get('list') == [1, 2, 3, 4]
        doc.get('subDoc') == subDoc

        clone.getObjectId('objectId') != objectId
        clone.getDate('date') != date
        clone.get('list') == [2, 3, 4, 5]
        clone.get('subDoc') == ['a': 2, 'b': '3', 'c': [2, 3, 4, 5]] as Document
    }

    def 'should throw a ClassCastException for invalid type'() {
        given:
        Worker pete = new Worker('Pete', 'handyman', new Date(), 3)
        Document doc = ['worker': pete] as Document

        when:
        new Document(doc)

        then:
        thrown(ClassCastException)
    }

}
