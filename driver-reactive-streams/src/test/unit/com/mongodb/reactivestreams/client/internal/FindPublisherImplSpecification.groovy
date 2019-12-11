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

package com.mongodb.reactivestreams.client.internal

import com.mongodb.CursorType
import com.mongodb.async.client.FindIterable
import com.mongodb.client.model.Collation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.reactivestreams.Subscriber
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class FindPublisherImplSpecification extends Specification {

    def 'should call the underlying wrapped methods'() {
        given:
        def sort = new Document('sort', 1)
        def projection = new Document('projection', 1)
        def collation = Collation.builder().locale('en').build()
        def batchSize = 100

        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(batchSize) }
        }

        def wrapped = Mock(FindIterable)
        def publisher = new FindPublisherImpl<Document>(wrapped)

        when:
        publisher.subscribe(subscriber)

        then:
        1 * wrapped.batchCursor(_)

        when: 'setting options'
        publisher = publisher
                .sort(sort)
                .projection(projection)
                .maxTime(1, TimeUnit.SECONDS)
                .maxAwaitTime(2, TimeUnit.SECONDS)
                .limit(100)
                .skip(10)
                .cursorType(CursorType.NonTailable)
                .oplogReplay(false)
                .noCursorTimeout(false)
                .partial(false)
                .collation(collation)
                .comment('my comment')
                .min(new BsonDocument('x', new BsonInt32(1)))
                .max(new BsonDocument('x', new BsonInt32(5)))
                .hint(new BsonDocument('y', new BsonInt32(1)))
                .hintString('a_1')
                .returnKey(true)
                .showRecordId(true)

        then:
        1 * wrapped.sort(sort) >> wrapped
        1 * wrapped.projection(projection) >> wrapped
        1 * wrapped.maxTime(1, TimeUnit.SECONDS) >> wrapped
        1 * wrapped.maxAwaitTime(2, TimeUnit.SECONDS) >> wrapped
        1 * wrapped.limit(100) >> wrapped
        1 * wrapped.skip(10) >> wrapped
        1 * wrapped.cursorType(CursorType.NonTailable) >> wrapped
        1 * wrapped.oplogReplay(false) >> wrapped
        1 * wrapped.noCursorTimeout(false) >> wrapped
        1 * wrapped.partial(false) >> wrapped
        1 * wrapped.collation(collation) >> wrapped
        1 * wrapped.comment('my comment') >> wrapped
        1 * wrapped.min(new BsonDocument('x', new BsonInt32(1))) >> wrapped
        1 * wrapped.max(new BsonDocument('x', new BsonInt32(5))) >> wrapped
        1 * wrapped.hint(new BsonDocument('y', new BsonInt32(1))) >> wrapped
        1 * wrapped.hintString('a_1') >> wrapped
        1 * wrapped.returnKey(true) >> wrapped
        1 * wrapped.showRecordId(true) >> wrapped

        when:
        publisher.subscribe(subscriber)

        then:
        1 * wrapped.batchSize(batchSize) >> wrapped
        1 * wrapped.batchCursor(_)

        when:
        publisher.first().subscribe(subscriber)

        then:
        1 * wrapped.first(_)

        when: 'setting batchSize'
        publisher.batchSize(10).subscribe(subscriber)

        then:
        1 * wrapped.batchSize(10) >> wrapped
        1 * wrapped.batchCursor(_)
    }

}
