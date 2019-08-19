/*
 * Copyright 2015 MongoDB, Inc.
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

import com.mongodb.async.client.AggregateIterable
import com.mongodb.client.model.Collation
import org.bson.Document
import org.reactivestreams.Subscriber
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class AggregatePublisherImplSpecification  extends Specification {

    def 'should call the underlying wrapped methods'() {
        given:
        def collation = Collation.builder().locale('en').build()
        def hint = new Document('a', 1)
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(100) }
        }

        def wrapped = Mock(AggregateIterable)
        def publisher = new AggregatePublisherImpl<Document>(wrapped)

        when:
        publisher.subscribe(subscriber)

        then:
        1 * wrapped.batchCursor(_)

        when: 'setting options'
        publisher = publisher
                .allowDiskUse(true)
                .bypassDocumentValidation(true)
                .collation(collation)
                .comment('a comment')
                .maxTime(1, TimeUnit.SECONDS)
                .maxAwaitTime(2, TimeUnit.SECONDS)
                .hint(hint)
                .useCursor(true)

        then:
        1 * wrapped.allowDiskUse(true) >> wrapped
        1 * wrapped.bypassDocumentValidation(true) >> wrapped
        1 * wrapped.collation(collation) >> wrapped
        1 * wrapped.comment('a comment') >> wrapped
        1 * wrapped.maxTime(1, TimeUnit.SECONDS) >> wrapped
        1 * wrapped.maxAwaitTime(2, TimeUnit.SECONDS) >> wrapped
        1 * wrapped.hint(hint) >> wrapped
        1 * wrapped.useCursor(true) >> wrapped

        when:
        publisher.subscribe(subscriber)

        then:
        1 * wrapped.batchCursor(_) >> wrapped

        when: 'setting batchSize'
        publisher.batchSize(10).subscribe(subscriber)

        then:
        1 * wrapped.batchSize(10)
        1 * wrapped.batchCursor(_)

        when:
        publisher.first().subscribe(subscriber)

        then:
        1 * wrapped.first(_)

        when: 'calling toCollection'
        publisher.toCollection()

        then:
        0 * wrapped.toCollection(_)

        when:
        publisher.toCollection().subscribe(subscriber)

        then:
        1 * wrapped.toCollection(_)
    }
}
