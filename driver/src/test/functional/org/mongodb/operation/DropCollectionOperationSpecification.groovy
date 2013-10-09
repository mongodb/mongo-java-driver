/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.CommandResult
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoException
import org.mongodb.MongoNamespace

import static java.lang.System.currentTimeMillis
import static org.mongodb.Fixture.bufferProvider
import static org.mongodb.Fixture.getMongoClient
import static org.mongodb.Fixture.session

class DropCollectionOperationSpecification extends FunctionalSpecification {

    def 'should not throw an Exception when the namespace is not found'() {
        given:
        def namespace = new MongoNamespace('non-existing-database' + currentTimeMillis(), 'coll')
        DropCollectionOperation operation = new DropCollectionOperation(
                namespace, bufferProvider, session, false)

        when:
        CommandResult commandResult = operation.execute()

        then:
        notThrown(MongoException)
        !commandResult.isOk()
        commandResult.errorMessage == 'ns not found'

        cleanup:
        getMongoClient().getDatabase(namespace.getDatabaseName()).tools().drop();
    }

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
