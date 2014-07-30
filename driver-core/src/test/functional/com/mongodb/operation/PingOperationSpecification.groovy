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
import org.junit.experimental.categories.Category

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding

class PingOperationSpecification extends OperationFunctionalSpecification {

    def 'should get a ping'() {

        when:
        PingOperation op = new PingOperation()
        def result = op.execute(getBinding());

        then:
        result >= 0.0
    }

    @Category(Async)
    def 'should get a ping asynchronously'() {

        when:
        PingOperation op = new PingOperation()
        def result = op.executeAsync(getAsyncBinding());

        then:
        result.get() >= 0.0
    }
}
