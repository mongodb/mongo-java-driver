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

package com.mongodb.async.rxjava.client

import org.mongodb.Document

import static Fixture.get

class SaveSpecification extends FunctionalSpecification {
    def 'save should upsert a document that has an _id'() {
        given:
        def document = new Document('_id', 1)

        when:
        get(collection.save(document))

        then:
        get(collection.find(new Document()).one()) == document

    }

    def 'save should insert a document that has no _id'() {
        given:
        def document = new Document()

        when:
        get(collection.save(document))

        then:
        get(collection.find(new Document()).one()) == document
    }
}