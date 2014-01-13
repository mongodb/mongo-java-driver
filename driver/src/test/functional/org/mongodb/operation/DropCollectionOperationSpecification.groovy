/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.operation

import org.mongodb.Document
import org.mongodb.FunctionalSpecification

import static org.mongodb.Fixture.bufferProvider
import static org.mongodb.Fixture.session

class DropCollectionOperationSpecification extends FunctionalSpecification {


    def 'should drop a collection that exists'() {
        given:
        collection.insert(new Document('documentTo', 'createTheCollection'))
        assert collectionName in database.tools().collectionNames

        when:
        new DropCollectionOperation(getNamespace(), bufferProvider, session, false).execute()

        then:
        !(collectionName in database.tools().collectionNames)
    }
}
