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

package com.mongodb.async.client

import com.mongodb.Function
import com.mongodb.MongoException
import com.mongodb.async.FutureResultCallback
import com.mongodb.async.AsyncBatchCursor
import org.bson.Document
import spock.lang.Specification

class MappingAsyncBatchCursorSpecification extends Specification {
    def cannedResults = [new Document('name', 'a'), new Document('name', 'b'), new Document('name', 'c')]
    def asyncBatchCursor = {
        Stub(AsyncBatchCursor) {
            def count = 0
            def results;
            def getResult = {
                count++
                results = count == 1 ? cannedResults : null
                results
            }
            next(_) >> {
                it[0].onResult(getResult(), null)
            }
            isClosed() >> { count >= 1 }
        }
    }

    def 'should map types from the cursor'() {
        given:
        def mapper = new Function<Document, String>() {
            @Override
            String apply(final Document document) {
                document.getString('name')
            }
        }
        def mappingAsyncBatchCursor = new MappingAsyncBatchCursor(asyncBatchCursor(), mapper)

        when:
        def results = new FutureResultCallback()
        mappingAsyncBatchCursor.next(results)

        then:
        results.get() == cannedResults*.name

        when:
        results = new FutureResultCallback()
        mappingAsyncBatchCursor.next(results)
        !mappingAsyncBatchCursor.isClosed()

        then:
        results.get() == null
        mappingAsyncBatchCursor.isClosed()
    }

    def 'should capture mapping errors'() {
        given:
        def mapper = new Function<Document, String>() {
            @Override
            String apply(final Document document) {
                throw new MongoException('Something went wrong')
            }
        }
        def mappingAsyncBatchCursor = new MappingAsyncBatchCursor(asyncBatchCursor(), mapper)

        when:
        def results = new FutureResultCallback()
        mappingAsyncBatchCursor.next(results)
        results.get()

        then:
        thrown MongoException
    }

    def 'should proxy the underlying asyncBatchCursor'() {
        given:
        def asyncBatchCursor = Mock(AsyncBatchCursor)
        def mapper = Stub(Function)
        def mappingAsyncBatchCursor = new MappingAsyncBatchCursor(asyncBatchCursor, mapper)

        when:
        mappingAsyncBatchCursor.setBatchSize(10)

        then:
        1 * asyncBatchCursor.setBatchSize(10)

        when:
        mappingAsyncBatchCursor.getBatchSize()

        then:
        1 * asyncBatchCursor.getBatchSize()

        when:
        mappingAsyncBatchCursor.isClosed()

        then:
        1 * asyncBatchCursor.isClosed()

        when:
        mappingAsyncBatchCursor.close()

        then:

        1 * asyncBatchCursor.close()
    }

}
