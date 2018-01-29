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

package com.mongodb

import org.bson.Document
import spock.lang.Specification


class DocumentToDBRefTransformerSpecification extends Specification {
    def transformer = new DocumentToDBRefTransformer()

    def 'should not transform a value that is not a Document'() {
        given:
        def str = 'some string'

        expect:
        transformer.transform(str).is(str)
    }

    def 'should not transform a Document that does not have both $ref and $id fields'() {
        expect:
        transformer.transform(doc).is(doc)

        where:
        doc << [new Document(),
                new Document('foo', 'bar'),
                new Document('$ref', 'bar'),
                new Document('$id', 'bar')]
    }

    def 'should transform a Document that has both $ref and $id fields to a DBRef'() {
        when:
        def doc = new Document('$ref', 'foo').append('$id', 1)

        then:
        transformer.transform(doc) == new DBRef('foo', 1)
    }

    def 'should transform a Document that has $ref and $id and $db fields to a DBRef'() {
        when:
        def doc = new Document('$ref', 'foo').append('$id', 1).append('$db', 'mydb')

        then:
        transformer.transform(doc) == new DBRef('mydb', 'foo', 1)
    }

    def 'should be equal to another instance'() {
        expect:
        transformer == new DocumentToDBRefTransformer()
    }

    def 'should not be equal to anything else'() {
        expect:
        transformer != 1
    }
}
