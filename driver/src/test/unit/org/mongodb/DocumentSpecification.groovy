/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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
        doc.getBoolean('boolean') == true;
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
        Document document = Document.valueOf("{'regex' : /abc/im }", JSONMode.JavaScript);

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

}
