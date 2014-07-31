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

import org.mongodb.Document

class RemoveSpecification extends FunctionalSpecification {
    def 'remove should remove all matching documents'() {
        given:
        def documents = [new Document('_id', 1).append('x', true),
                         new Document('_id', 2).append('x', true),
                         new Document('_id', 3).append('x', false)]
        collection.insert(documents).get()

        when:
        collection.find(new Document('x', true)).remove().get()

        then:
        collection.find(new Document()).into([]).get() == [documents[2]]
    }

    def 'removeOne should remove one matching document'() {
        given:
        def documents = [new Document('_id', 1).append('x', true),
                         new Document('_id', 2).append('x', true),
                         new Document('_id', 3).append('x', true)]
        collection.insert(documents).get()

        when:
        collection.find(new Document('x', true)).removeOne().get()

        then:
        collection.find(new Document()).count().get() == 2
    }

}