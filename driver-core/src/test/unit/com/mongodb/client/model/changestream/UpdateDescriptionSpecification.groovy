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

package com.mongodb.client.model.changestream

import org.bson.BsonDocument
import spock.lang.Specification

import static java.util.Collections.emptyList
import static java.util.Collections.singletonList

class UpdateDescriptionSpecification extends Specification {

    def 'should create the expected UpdateDescription'() {
       when:
       def description = new UpdateDescription(removedFields, updatedFields, truncatedArrays)

        then:
        description.getRemovedFields() == removedFields
        description.getUpdatedFields() == updatedFields
        description.getTruncatedArrays() == (truncatedArrays ? truncatedArrays : emptyList())

        where:
        removedFields | updatedFields                | truncatedArrays
        ['a', 'b']    | null                         | null
        null          | BsonDocument.parse('{c: 1}') | []
        ['a', 'b']    | BsonDocument.parse('{c: 1}') | singletonList(new TruncatedArray('d', 1))
    }
}
