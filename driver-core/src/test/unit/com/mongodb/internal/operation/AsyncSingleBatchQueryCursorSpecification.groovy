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

package com.mongodb.internal.operation

import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ServerAddress
import com.mongodb.async.FutureResultCallback
import com.mongodb.internal.connection.QueryResult
import org.bson.Document
import spock.lang.Specification

import static com.mongodb.ClusterFixture.DEFAULT_CSOT_FACTORY

class AsyncSingleBatchQueryCursorSpecification extends Specification {

    def 'should work as expected'() {
        given:
        def cursor = new AsyncSingleBatchQueryCursor<Document>(DEFAULT_CSOT_FACTORY.create(), firstBatch)

        when:
        def batch = nextBatch(cursor)

        then:
        batch == firstBatch.getResults()

        then:
        nextBatch(cursor) == null

        when:
        nextBatch(cursor)

        then:
        thrown(MongoException)
    }

    def 'should work as with tryNext'() {
        given:
        def cursor = new AsyncSingleBatchQueryCursor<Document>(DEFAULT_CSOT_FACTORY.create(), firstBatch)

        when:
        def batch = tryNextBatch(cursor)

        then:
        batch == firstBatch.getResults()

        then:
        tryNextBatch(cursor) == null

        when:
        tryNextBatch(cursor)

        then:
        thrown(MongoException)
    }

    def 'should not support setting batchsize'() {
        given:
        def cursor = new AsyncSingleBatchQueryCursor<Document>(DEFAULT_CSOT_FACTORY.create(), firstBatch)

        when:
        cursor.setBatchSize(1)

        then:
        cursor.getBatchSize() == 0
    }


    List<Document> nextBatch(AsyncSingleBatchQueryCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.next(futureResultCallback)
        futureResultCallback.get()
    }
    List<Document> tryNextBatch(AsyncSingleBatchQueryCursor cursor) {
        def futureResultCallback = new FutureResultCallback()
        cursor.tryNext(futureResultCallback)
        futureResultCallback.get()
    }


    def firstBatch = new QueryResult(new MongoNamespace('db', 'coll'), [new Document('a', 1)], 0, new ServerAddress())
}
