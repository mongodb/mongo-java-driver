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

import org.bson.Document

class ReplaceSpecification extends FunctionalSpecification {
     def 'should replace a document'() {
         given:
         collection.insert([new Document('_id', 1), new Document('_id', 2)]).get()
         def filter = new Document('_id', 2)
         def replacement = new Document('_id', 2).append('x', 1)

         when:
         collection.find(filter).replace(replacement).get()

         then:
         collection.find(filter).one().get() == replacement
     }
}