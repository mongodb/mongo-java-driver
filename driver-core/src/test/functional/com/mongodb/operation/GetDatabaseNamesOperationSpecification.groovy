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

package com.mongodb.operation

import category.Async
import com.mongodb.OperationFunctionalSpecification
import org.bson.codecs.DocumentCodec
import org.bson.types.Document
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static java.util.concurrent.TimeUnit.SECONDS

class GetDatabaseNamesOperationSpecification extends OperationFunctionalSpecification {

    def 'should return a list of database names'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new GetDatabaseNamesOperation()

        when:
        List<String> names = operation.execute(getBinding())

        then:
        names.contains(getDatabaseName())
    }

    @Category(Async)
    def 'should return a list of database names asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new GetDatabaseNamesOperation()

        when:
        List<String> names = operation.executeAsync(getAsyncBinding()).get(1, SECONDS)

        then:
        names.contains(getDatabaseName())
    }

}
