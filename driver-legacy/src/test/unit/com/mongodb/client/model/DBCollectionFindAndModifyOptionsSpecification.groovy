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
import com.mongodb.WriteConcern
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class DBCollectionFindAndModifyOptionsSpecification extends Specification {

    def 'should have the expected default values'() {
        when:
        def options = new DBCollectionFindAndModifyOptions()

        then:
        !options.isRemove()
        !options.isUpsert()
        !options.returnNew()
        options.getBypassDocumentValidation() == null
        options.getCollation() == null
        options.getArrayFilters() == null
        options.getMaxTime(TimeUnit.MILLISECONDS) == 0
        options.getProjection() == null
        options.getSort() == null
        options.getUpdate() == null
        options.getWriteConcern() == null
    }

    def 'should set and return the expected values'() {
        given:
        def collation = Collation.builder().locale('en').build()
        def writeConcern = WriteConcern.MAJORITY
        def projection = BasicDBObject.parse('{a: 1, _id: 0}')
        def sort = BasicDBObject.parse('{a: 1}')
        def update = BasicDBObject.parse('{$set: {a:  2}}')
        def arrayFilters = [new BasicDBObject('i.b', 1)]

        when:
        def options = new DBCollectionFindAndModifyOptions()
                .bypassDocumentValidation(true)
                .collation(collation)
                .maxTime(1, TimeUnit.MILLISECONDS)
                .projection(projection)
                .remove(true)
                .returnNew(true)
                .sort(sort)
                .update(update)
                .upsert(true)
                .arrayFilters(arrayFilters)
                .writeConcern(writeConcern)

        then:
        options.getBypassDocumentValidation()
        options.getCollation() == collation
        options.getMaxTime(TimeUnit.MILLISECONDS) == 1
        options.getProjection() == projection
        options.getSort() == sort
        options.getUpdate() == update
        options.getWriteConcern() == writeConcern
        options.isRemove()
        options.isUpsert()
        options.returnNew()
        options.arrayFilters(arrayFilters)
    }

}
