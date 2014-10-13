/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.async.client

import org.bson.types.Document

class CountSpecification extends FunctionalSpecification {

    def 'count should be zero when the collection is empty'() {
        expect:
        collection.find(new Document()).count().get() == 0
    }

    def 'count should be one when the collection contains one document'() {
        given:
        collection.insert(new Document()).get()

        expect:
        collection.find(new Document()).count().get() == 1
    }

    def 'count should be one when the collection contains one document that matches the filter'() {
        given:
        collection.insert(new Document('x', 1)).get()
        collection.insert(new Document('x', 2)).get()

        expect:
        collection.find(new Document('x', 1)).count().get() == 1
    }
}