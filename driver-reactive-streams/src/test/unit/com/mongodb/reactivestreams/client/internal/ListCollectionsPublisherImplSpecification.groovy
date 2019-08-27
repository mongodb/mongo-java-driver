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

import com.mongodb.internal.async.client.ListCollectionsIterable
import org.bson.Document
import org.reactivestreams.Subscriber
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class ListCollectionsPublisherImplSpecification extends Specification {

    def 'should call the underlying wrapped methods'() {
        given:
        def filter = new Document('field', 1)
        def subscriber = Stub(Subscriber) {
            onSubscribe(_) >> { args -> args[0].request(100) }
        }


        def wrapped = Mock(ListCollectionsIterable)
        def publisher = new ListCollectionsPublisherImpl(wrapped)

        when:
        publisher.subscribe(subscriber)

        then:
        1 * wrapped.batchCursor(_)

        when: 'setting options'
        publisher = publisher
                .filter(filter)
                .maxTime(1, TimeUnit.SECONDS)

        then:
        1 * wrapped.filter(filter) >> wrapped
        1 * wrapped.maxTime(1, TimeUnit.SECONDS) >> wrapped

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
