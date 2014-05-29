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

package org.mongodb.operation
import category.Async
import org.mongodb.CreateCollectionOptions
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.junit.experimental.categories.Category

import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding

class GetIsCappedOperationSpecification extends FunctionalSpecification {

    def 'should be false for normal collection'() {
        given:
        getCollectionHelper().insertDocuments(new Document('documentTo', 'createTheCollection'))
        def operation = new GetIsCappedOperation(getNamespace())

        when:
        Boolean isCapped = operation.execute(getBinding())

        then:
        !isCapped
    }

    @Category(Async)
    def 'should be false for normal collection asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new Document('documentTo', 'createTheCollection'))
        def operation = new GetIsCappedOperation(getNamespace())

        when:
        Boolean isCapped = operation.executeAsync(getAsyncBinding()).get()

        then:
        !isCapped
    }

    def 'should be true for capped collection'() {
        given:
        new CreateCollectionOperation(getDatabaseName(), new CreateCollectionOptions(getCollectionName(), true, 1024)).execute(getBinding())
        def operation = new GetIsCappedOperation(getNamespace())

        when:
        Boolean isCapped = operation.execute(getBinding())

        then:
        isCapped
    }

    @Category(Async)
    def 'should be true for capped collection asynchronously'() {
        given:
        new CreateCollectionOperation(getDatabaseName(), new CreateCollectionOptions(getCollectionName(), true, 1024)).execute(getBinding())
        def operation = new GetIsCappedOperation(getNamespace())

        when:
        Boolean isCapped = operation.executeAsync(getAsyncBinding()).get()

        then:
        isCapped
    }

    def 'should not error getting isCapped for nonexistent collection'() {
        given:
        def operation = new GetIsCappedOperation(getNamespace())

        when:
        Boolean isCapped = operation.execute(getBinding())

        then:
        !isCapped
    }

    @Category(Async)
    def 'should not error getting isCapped for nonexistent collection asynchronously'() {
        given:
        def operation = new GetIsCappedOperation(getNamespace())

        when:
        Boolean isCapped = operation.executeAsync(getAsyncBinding()).get()

        then:
        !isCapped
    }
}
