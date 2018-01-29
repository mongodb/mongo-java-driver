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

class DBCollectionDistinctOptionsSpecification extends Specification {

    def 'should have the expected default values'() {
        when:
        def options = new DBCollectionDistinctOptions()

        then:
        options.getCollation() == null
        options.getFilter() == null
        options.getReadConcern() == null
        options.getReadPreference() == null
    }

    def 'should set and return the expected values'() {
        given:
        def collation = Collation.builder().locale('en').build()
        def readConcern = ReadConcern.LOCAL
        def readPreference = ReadPreference.nearest()
        def filter = BasicDBObject.parse('{a: 1}')

        when:
        def options = new DBCollectionDistinctOptions()
                .collation(collation)
                .filter(filter)
                .readConcern(readConcern)
                .readPreference(readPreference)

        then:
        options.getCollation() == collation
        options.getFilter() == filter
        options.getReadConcern() == readConcern
        options.getReadPreference() == readPreference
    }

}
