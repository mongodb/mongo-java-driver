/*
 * Copyright 2018-present MongoDB, Inc.
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
 *
 */

package com.mongodb.client.model

import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

import static com.mongodb.client.model.AggregateOutStageOptions.Mode.REPLACE_COLLECTION
import static com.mongodb.client.model.AggregateOutStageOptions.Mode.REPLACE_DOCUMENTS

class AggregateOutStageOptionsSpecification extends Specification {
    def 'test options'() {
        when:
        def options = new AggregateOutStageOptions()

        then:
        options.getMode() == REPLACE_COLLECTION
        options.getDatabaseName() == null
        options.getUniqueKey() == null

        when:
        options.mode(REPLACE_DOCUMENTS)
        options.databaseName('db1')
        options.uniqueKey(new BsonDocument('x', new BsonInt32(1)))

        then:
        options.getMode() == REPLACE_DOCUMENTS
        options.getDatabaseName() == 'db1'
        options.getUniqueKey() == new BsonDocument('x', new BsonInt32(1))
    }
}
