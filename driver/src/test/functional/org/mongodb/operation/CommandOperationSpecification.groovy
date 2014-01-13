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
import org.mongodb.Fixture
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoExecutionTimeoutException
import org.mongodb.ReadPreference
import org.mongodb.codecs.DocumentCodec

import static java.util.Arrays.asList
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.bufferProvider
import static org.mongodb.Fixture.disableMaxTimeFailPoint
import static org.mongodb.Fixture.enableMaxTimeFailPoint
import static org.mongodb.Fixture.serverVersionAtLeast
import static org.mongodb.Fixture.session

class CommandOperationSpecification extends FunctionalSpecification {
    def 'should throw execution timeout exception from execute'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));

        given:
        def commandOperation = new CommandOperation(getNamespace().databaseName,
                                                    new Document('count', getCollectionName()).append('maxTimeMS', 1),
                                                    ReadPreference.primary(), new DocumentCodec(), new DocumentCodec(), bufferProvider,
                                                    session, true)
        enableMaxTimeFailPoint()

        when:
        commandOperation.execute()

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    def 'should throw execution timeout exception from executeAsync'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
            assumeTrue(Fixture.mongoClientURI.options.isAsyncEnabled())

        given:
        def commandOperation = new CommandOperation(getNamespace().databaseName,
                                                    new Document('count', getCollectionName()).append('maxTimeMS', 1),
                                                    ReadPreference.primary(), new DocumentCodec(), new DocumentCodec(), bufferProvider,
                                                    session, true)
        enableMaxTimeFailPoint()

        when:
        commandOperation.executeAsync().get()

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }
}