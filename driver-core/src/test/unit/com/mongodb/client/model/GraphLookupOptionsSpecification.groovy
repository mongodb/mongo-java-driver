/*
 * Copyright 2016 MongoDB, Inc.
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

import spock.lang.Specification

import static org.bson.BsonDocument.parse

class GraphLookupOptionsSpecification extends Specification {
    def "should return new options with the same property values"() {
        when:
        def options = new GraphLookupOptions()
                .maxDepth(10)
                .depthField('field')
                .restrictSearchWithMatch(parse('{x : 1}'))

        then:
        options.maxDepth == 10
        options.depthField == 'field'
        options.restrictSearchWithMatch == parse('{x : 1}')
    }
}
