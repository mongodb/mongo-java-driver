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

package com.mongodb.client.model

import org.bson.BsonDocument
import org.bson.Document
import spock.lang.Specification

class DeleteOptionsSpecification extends Specification {

    def 'should have the expected defaults'() {
        when:
        def options = new DeleteOptions()

        then:
        options.getCollation() == null
        options.getHint() == null
        options.getHintString() == null
    }

    def 'should set collation'() {
        expect:
        new DeleteOptions().collation(collation).getCollation() == collation

        where:
        collation << [null, Collation.builder().locale('en').build()]
    }

    def 'should set hint'() {
        expect:
        new DeleteOptions().hint(hint).getHint() == hint

        where:
        hint << [null, new BsonDocument(), new Document('a', 1)]
    }

    def 'should set hintString'() {
        expect:
        new DeleteOptions().hintString(hintString).getHintString() == hintString

        where:
        hintString << [null, 'a_1']
    }
}
