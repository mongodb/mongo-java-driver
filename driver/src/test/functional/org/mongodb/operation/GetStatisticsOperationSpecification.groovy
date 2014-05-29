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
import org.junit.experimental.categories.Category
import org.mongodb.Document
import org.mongodb.FunctionalSpecification

import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getBinding

class GetStatisticsOperationSpecification extends FunctionalSpecification {

    def 'should get statistics'() {
        given:
        getCollectionHelper().insertDocuments(new Document('documentTo', 'createTheCollection'))
        def operation = new GetStatisticsOperation(getNamespace())

        when:
        Document statistics = operation.execute(getBinding())

        then:
        statistics.getInteger('count') == 1
    }

    @Category(Async)
    def 'should get statistics asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new Document('documentTo', 'createTheCollection'))
        def operation = new GetStatisticsOperation(getNamespace())

        when:
        Document statistics = operation.executeAsync(getAsyncBinding()).get()

        then:
        statistics.getInteger('count') == 1
    }

    def 'should not error getting statistics for nonexistent collection'() {
        given:
        def operation = new GetStatisticsOperation(getNamespace())

        when:
        Document statistics = operation.execute(getBinding())

        then:
        !statistics.isEmpty()
    }

    @Category(Async)
    def 'should not error getting statistics for nonexistent collection asynchronously'() {
        given:
        def operation = new GetStatisticsOperation(getNamespace())

        when:
        Document statistics = operation.executeAsync(getAsyncBinding()).get()

        then:
        !statistics.isEmpty()
    }
}
