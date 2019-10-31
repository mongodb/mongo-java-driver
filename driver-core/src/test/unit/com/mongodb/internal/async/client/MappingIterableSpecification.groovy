/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.async.client

import com.mongodb.Block
import com.mongodb.Function
import com.mongodb.internal.async.SingleResultCallback
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class MappingIterableSpecification extends Specification {

    def 'should follow the MongoIterable interface as expected'() {
        given:
        def callback = Stub(SingleResultCallback)
        def iterable = Mock(AsyncMongoIterable)
        def mapper = { doc -> doc }
        def mappingIterable = new MappingIterable(iterable, mapper)

        when:
        mappingIterable.first(callback)

        then:
        1 * iterable.first(_)

        when:
        mappingIterable.forEach( { } as Block, callback)

        then:
        1 * iterable.forEach(_, _)

        when:
        mappingIterable.into([], callback)

        then:
        1 * iterable.forEach(_, _)  // Use foreach to populate the target

        when:
        mappingIterable.batchSize(5)

        then:
        1 * iterable.batchSize(5)

        when:
        mappingIterable.getBatchSize()

        then:
        1 * iterable.getBatchSize() >> 5

        when:
        mappingIterable.batchCursor(callback)

        then:
        1 * iterable.batchCursor(_)

        when:
        def newMapper = { } as Function

        then:
        expect mappingIterable.map(newMapper), isTheSameAs(new MappingIterable(mappingIterable, newMapper))
    }

}
