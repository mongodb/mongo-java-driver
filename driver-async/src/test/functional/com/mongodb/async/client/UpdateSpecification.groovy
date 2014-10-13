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
import org.mongodb.WriteResult

class UpdateSpecification extends FunctionalSpecification {
     def 'update should update all matching documents'() {
         given:
         def documents = [new Document('_id', 1).append('x', true), new Document('_id', 2).append('x', true)]
         collection.insert(documents).get()

         when:
         collection.find(new Document('x', true)).update(new Document('$set', new Document('y', false))).get()

         then:
         collection.find(new Document('x', true)).sort(new Document('_id', 1)).into([]).get() ==
         documents.each { it.append('y', false) }
     }

    def 'update with upsert should insert a single document if there are no matching documents'() {
        when:
        WriteResult result = collection.find(new Document('x', true)).upsert().update(new Document('$set', new Document('y', false))).get()

        then:
        collection.find(new Document()).into([]).get() ==
        [new Document('_id', result.upsertedId.asObjectId().getValue()).append('x', true).append('y', false)]
    }

    def 'updateOne should update one matching document'() {
        given:
        def document = new Document('_id', 1).append('x', true)
        collection.insert(document).get()

        when:
        collection.find(new Document('x', true)).updateOne(new Document('$set', new Document('y', false))).get()

        then:
        collection.find(new Document('y', false)).into([]).get() == [document.append('y', false)]
    }

    def 'updateOne should update one of many matching documents'() {
        given:
        def documents = [new Document('_id', 1).append('x', true), new Document('_id', 2).append('x', true)]
        collection.insert(documents).get()

        when:
        collection.find(new Document('x', true)).updateOne(new Document('$set', new Document('y', false))).get()

        then:
        collection.find(new Document('y', false)).count().get() == 1
    }

    def 'updateOne with upsert should insert a document if there are no matching documents'() {
        given:
        def document = new Document('x', true)

        when:
        def result = collection.find(new Document('x', true)).upsert().updateOne(new Document('$set', new Document('y', false))).get()

        then:
        collection.find(new Document('y', false)).into([]).get() ==
        [document.append('_id', result.getUpsertedId().asObjectId().getValue()).append('y', false)]
    }
}