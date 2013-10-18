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

import org.mongodb.AsyncBlock
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoExecutionTimeoutException
import org.mongodb.codecs.DocumentCodec

import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.bufferProvider
import static org.mongodb.Fixture.disableMaxTimeFailPoint
import static org.mongodb.Fixture.enableMaxTimeFailPoint
import static org.mongodb.Fixture.serverVersionAtLeast
import static org.mongodb.Fixture.session

class QueryOperationSpecification extends FunctionalSpecification {

    @Override
    def setup() {
        collection.insert(new Document())
    }

    def 'should throw execution timeout exception from execute'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));

        given:
        def find = new Find().maxTime(1, SECONDS)
        def queryOperation = new QueryOperation<Document>(getNamespace(), find, new DocumentCodec(), new DocumentCodec(),
                                                          bufferProvider, session, true)
        enableMaxTimeFailPoint()

        when:
        queryOperation.execute()

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    def 'should throw execution timeout exception from executeAsync'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));

        given:
        def find = new Find().maxTime(1, SECONDS)
        def queryOperation = new QueryOperation<Document>(getNamespace(), find, new DocumentCodec(), new DocumentCodec(),
                                                          bufferProvider, session, true)
        def runCalled = false
        enableMaxTimeFailPoint()

        when:
        queryOperation.executeAsync().get().start(new AsyncBlock<Document>() {
            @Override
            void done() {
            }

            @Override
            boolean run(final Document t) {
                runCalled = true
            }
        })

        then:
        !runCalled
        // thrown(MongoExecutionTimeoutException)  TODO: enable this when MongoAsyncCursor is able to indicate exceptions

        cleanup:
        disableMaxTimeFailPoint()
    }
}