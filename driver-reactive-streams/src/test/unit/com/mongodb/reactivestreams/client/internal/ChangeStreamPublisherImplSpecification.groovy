/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.reactivestreams.client.internal

import com.mongodb.client.model.Collation
import com.mongodb.internal.async.client.AsyncChangeStreamIterable
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonTimestamp
import org.bson.Document
import org.reactivestreams.Subscriber
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.client.model.changestream.FullDocument.UPDATE_LOOKUP

class ChangeStreamPublisherImplSpecification extends Specification {

    def 'should call the underlying wrapped methods'() {
        given:
        def collation = Collation.builder().locale('en').build()
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(100) }
        }

        def wrapped = Mock(AsyncChangeStreamIterable)
        def publisher = new ChangeStreamPublisherImpl<Document>(wrapped)

        when:
        publisher.subscribe(subscriber)

        then:
        1 * wrapped.batchCursor(_)

        when: 'setting options'
        publisher = publisher
                .fullDocument(UPDATE_LOOKUP)
                .resumeAfter(new BsonDocument('_id', new BsonInt32(4)))
                .startAfter(new BsonDocument('_id', new BsonInt32(5)))
                .startAtOperationTime(new BsonTimestamp(42, 1))
                .collation(collation)
                .maxAwaitTime(2, TimeUnit.SECONDS)

        then:
        1 * wrapped.fullDocument(UPDATE_LOOKUP) >> wrapped
        1 * wrapped.resumeAfter(new BsonDocument('_id', new BsonInt32(4))) >> wrapped
        1 * wrapped.startAfter(new BsonDocument('_id', new BsonInt32(5))) >> wrapped
        1 * wrapped.startAtOperationTime(new BsonTimestamp(42, 1)) >> wrapped
        1 * wrapped.collation(collation) >> wrapped
        1 * wrapped.maxAwaitTime(2, TimeUnit.SECONDS) >> wrapped

        when:
        publisher.subscribe(subscriber)

        then:
        1 * wrapped.batchCursor(_)

        when: 'setting batchSize'
        publisher.batchSize(10).subscribe(subscriber)

        then:
        1 * wrapped.batchSize(10) >> wrapped
        1 * wrapped.batchCursor(_)

        when:
        publisher.first().subscribe(subscriber)

        then:
        1 * wrapped.first(_)
    }
}
