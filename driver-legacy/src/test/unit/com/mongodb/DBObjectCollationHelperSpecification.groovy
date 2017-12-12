/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb

import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationAlternate
import com.mongodb.client.model.CollationCaseFirst
import com.mongodb.client.model.CollationMaxVariable
import com.mongodb.client.model.CollationStrength
import spock.lang.Specification

class DBObjectCollationHelperSpecification extends Specification {

    def 'should create the expected collation'() {
        expect:
        DBObjectCollationHelper.createCollationFromOptions(new BasicDBObject('collation', BasicDBObject.parse(options))) == collation

        where:
        collation                                | options
        Collation.builder().locale('en').build() | '{locale: "en"}'
        Collation.builder()
                .locale('en')
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .normalization(true)
                .backwards(true)
                .build()                         | '''{locale: "en", caseLevel: true, caseFirst: "off", strength: 5,
                                                                  numericOrdering: true, alternate: "shifted",
                                                                  maxVariable: "space", normalization: true, backwards: true}'''
    }

    def 'should return null if no options are set'() {
        DBObjectCollationHelper.createCollationFromOptions(new BasicDBObject()) == null
    }

    def 'should throw an exception if the collation options are invalid'() {
        when:
        DBObjectCollationHelper.createCollationFromOptions(new BasicDBObject('collation', BasicDBObject.parse(options)))

        then:
        thrown(IllegalArgumentException)

        where:
        options << ['{}',
                    '{locale: true}',
                    '{ locale: "en", caseLevel: "true"}',
                    '{ locale: "en", caseFirst: false}',
                    '{ locale: "en", strength: true }',
                    '{ locale: "en", numericOrdering: 1}',
                    '{ locale: "en", alternate: true}',
                    '{ locale: "en", maxVariable: true}',
                    '{ locale: "en", normalization: 1}',
                    '{ locale: "en", backwards: 1}']
    }
}
