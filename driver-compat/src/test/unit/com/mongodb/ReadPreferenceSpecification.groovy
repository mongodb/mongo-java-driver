/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import org.mongodb.connection.Tags
import spock.lang.Specification

@SuppressWarnings(['DuplicateMapLiteral', 'LineLength'])
class ReadPreferenceSpecification extends Specification {

    def setupSpec() {
        Map.metaClass.bitwiseNegate = { new BasicDBObject(delegate) }
    }


    def 'should convert to correct documents'() {
        expect:
        readPreference.toDBObject() == document

        where:
        readPreference                                                | document
        ReadPreference.primary()                                      | ['mode': 'primary']
        ReadPreference.primaryPreferred()                             | ['mode': 'primaryPreferred']
        ReadPreference.primaryPreferred(~['dc': 'ny', 'rack': '1'])   | ['mode': 'primaryPreferred', 'tags': [~['dc': 'ny', 'rack': '1']]]
        ReadPreference.secondary()                                    | ['mode': 'secondary']
        ReadPreference.secondary(~['dc': 'ny'])                       | ['mode': 'secondary', 'tags': [~['dc': 'ny']]]
        ReadPreference.secondaryPreferred()                           | ['mode': 'secondaryPreferred']
        ReadPreference.secondaryPreferred(~['dc': 'ca', 'rack': '2']) | ['mode': 'secondaryPreferred', 'tags': [~['dc': 'ca', 'rack': '2']]]
        ReadPreference.nearest()                                      | ['mode': 'nearest']
        ReadPreference.nearest(~['dc': 'ny'])                         | ['mode': 'nearest', 'tags': [~['dc': 'ny']]]
    }

    def 'should have correct names'() {
        expect:
        readPreference.getName() == name

        where:
        readPreference                      | name
        ReadPreference.primary()            | 'primary'
        ReadPreference.primaryPreferred()   | 'primaryPreferred'
        ReadPreference.secondary()          | 'secondary'
        ReadPreference.secondaryPreferred() | 'secondaryPreferred'
        ReadPreference.nearest()            | 'nearest'
    }

    def 'should provide factory methods to construct from o.m.ReadPreference'() {
        expect:
        original == ReadPreference.fromNew(recent)

        where:
        original                                                      | recent
        ReadPreference.primary()                                      | org.mongodb.ReadPreference.primary()
        ReadPreference.primaryPreferred()                             | org.mongodb.ReadPreference.primaryPreferred()
        ReadPreference.primaryPreferred(~['dc': 'ny'])                | org.mongodb.ReadPreference.primaryPreferred(new Tags('dc', 'ny'))
        ReadPreference.secondary()                                    | org.mongodb.ReadPreference.secondary()
        ReadPreference.secondaryPreferred(~['dc': 'ca', 'rack': '2']) | org.mongodb.ReadPreference.secondaryPreferred(new Tags('dc', 'ca').append('rack', '2'))
        ReadPreference.nearest()                                      | org.mongodb.ReadPreference.nearest()
    }

    def 'should return a list of tags'() {
        given:
        ReadPreference readPreference = ReadPreference.nearest(~['dc': 'ny', 'rack': '1'], ~['z': 'usa'], ~['p': 'east'])

        when:
        List<DBObject> tags = readPreference.getTagSets()

        then:
        tags == [~['dc': 'ny', 'rack': '1'], ~['z': 'usa'], ~['p': 'east']]

        when:
        tags.add(~['dc': 'MA'])

        then:
        thrown(UnsupportedOperationException)
    }


}
