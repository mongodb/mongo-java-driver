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

import com.mongodb.BasicDBObject
import com.mongodb.ReadConcern
import com.mongodb.ReadPreference
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class DBCollectionCountOptionsSpecification extends Specification {

    def 'should have the expected default values'() {
        when:
        def options = new DBCollectionCountOptions()

        then:
        options.getCollation() == null
        options.getHint() == null
        options.getHintString() == null
        options.getLimit() == 0
        options.getMaxTime(TimeUnit.MILLISECONDS) == 0
        options.getReadConcern() == null
        options.getReadPreference() == null
        options.getSkip() == 0
    }

    def 'should set and return the expected values'() {
        given:
        def collation = Collation.builder().locale('en').build()
        def readConcern = ReadConcern.LOCAL
        def readPreference = ReadPreference.nearest()
        def hint = BasicDBObject.parse('{a: 1}')
        def hintString = 'a_1'

        when:
        def options = new DBCollectionCountOptions()
                .collation(collation)
                .hint(hint)
                .hintString(hintString)
                .limit(1)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .readConcern(readConcern)
                .readPreference(readPreference)
                .skip(1)

        then:
        options.getCollation() == collation
        options.getHint() == hint
        options.getHintString() == hintString
        options.getLimit() == 1
        options.getMaxTime(TimeUnit.MILLISECONDS) == 1
        options.getReadConcern() == readConcern
        options.getReadPreference() == readPreference
        options.getSkip() == 1
    }

}
