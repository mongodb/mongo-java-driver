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
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoExecutionTimeoutException
import org.mongodb.codecs.DocumentCodec

import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.disableMaxTimeFailPoint
import static org.mongodb.Fixture.enableMaxTimeFailPoint
import static org.mongodb.Fixture.getSession
import static org.mongodb.Fixture.serverVersionAtLeast

class CountOperationSpecification extends FunctionalSpecification {
    def 'should throw execution timeout exception from execute'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)))

        given:
        def find = new Find().maxTime(1, SECONDS)
        def countOperation = new CountOperation(getNamespace(), find, new DocumentCodec())
        enableMaxTimeFailPoint()

        when:
        countOperation.execute(getSession())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    def 'should throw execution timeout exception from executeAsync'() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)))

        given:
        def find = new Find().maxTime(1, SECONDS)
        def countOperation = new CountOperation(getNamespace(), find, new DocumentCodec())
        enableMaxTimeFailPoint()

        when:
        countOperation.executeAsync(getSession()).get()

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }
}
