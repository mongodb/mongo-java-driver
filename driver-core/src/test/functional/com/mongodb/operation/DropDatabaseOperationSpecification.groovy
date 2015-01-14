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
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded

class DropDatabaseOperationSpecification extends OperationFunctionalSpecification {

    @IgnoreIf({ isSharded() })
    def 'should drop a database that exists'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert databaseNameExists(databaseName)

        when:
        new DropDatabaseOperation(databaseName).execute(getBinding())

        then:
        !databaseNameExists(databaseName)
    }

    @Category(Async)
    @IgnoreIf({ isSharded() })
    def 'should drop a database that exists asynchronously'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('documentTo', 'createTheCollection'))
        assert databaseNameExists(databaseName)

        when:
        executeAsync(new DropDatabaseOperation(databaseName))

        then:
        !databaseNameExists(databaseName)
    }

    def 'should not error when dropping a collection that does not exist'() {
        given:
        def dbName = 'nonExistingDatabase'

        when:
        new DropDatabaseOperation(dbName).execute(getBinding())

        then:
        !databaseNameExists(dbName)
    }

    @Category(Async)
    def 'should not error when dropping a collection that does not exist asynchronously'() {
        given:
        def dbName = 'nonExistingDatabase'

        when:
        executeAsync(new DropDatabaseOperation(dbName))

        then:
        !databaseNameExists(dbName)
    }

    def databaseNameExists(String databaseName) {
        new ListDatabasesOperation(new DocumentCodec()).execute(getBinding()).next()*.name.contains(databaseName);
    }

}
