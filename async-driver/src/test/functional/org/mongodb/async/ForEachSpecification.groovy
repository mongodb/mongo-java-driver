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

package org.mongodb.async
import org.mongodb.Block
import org.mongodb.CancellationToken
import org.mongodb.Document
import org.mongodb.MongoFuture
import org.mongodb.MongoInternalException

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

class ForEachSpecification extends FunctionalSpecification {

    def 'should complete with no results'() {
        expect:
        collection.find(new Document()).forEach( { } as Block).get() == null
    }

    def 'should apply block and complete'() {
        given:
        def document = new Document()
        collection.insert(document).get()

        when:
        def queriedDocuments = []
        collection.find(new Document()).forEach( { doc -> queriedDocuments += doc } as Block).get()

        then:
        queriedDocuments == [document]
    }

    def 'should apply block for each document and then complete'() {
        given:
        def documents = [new Document(), new Document()]
        collection.insert(documents[0]).get()
        collection.insert(documents[1]).get()

        when:
        def queriedDocuments = []
        collection.find(new Document()).forEach( { doc -> queriedDocuments += doc } as Block).get()

        then:
        queriedDocuments == documents
    }

    def 'should throw MongoInternalException if apply throws'() {
        given:
        def document = new Document()
        collection.insert(document).get()

        when:
        collection.find(new Document()).forEach( { doc -> throw new IllegalArgumentException() } as Block).get()

        then:
        thrown(MongoInternalException)
    }

    def 'forEach should support future cancelled state'() {
        given:
        collection.insert((1..1000).collect { new Document('_id', it) }).get()

        AtomicInteger counter = new AtomicInteger()
        CountDownLatch latch = new CountDownLatch(5)
        Block<Document> cancelBlock = new Block<Document>() {
            @Override
            void apply(final Document document) {
                int it = counter.incrementAndGet()
                if (it == 5) {
                    latch.await()
                } else {
                    latch.countDown()
                }
            }
        }

        when:
        MongoFuture<Void> future = collection.find(new Document()).forEach(cancelBlock)
        sleep(1000)
        future.cancel(true)
        latch.countDown()

        then:
        latch.getCount() == 0
        counter.get() == 5
    }

    def 'forEach should terminate early in the CancellationToken has been cancelled'() {
        given:
        collection.insert((1..1000).collect { new Document('_id', it) })
        CancellationToken cancellationToken  = new CancellationToken();
        Block<Document> cancelBlock = new Block<Document>() {
            private int iterations = 0
            @Override
            void apply(final Document document) {
                iterations++
                if (iterations == 2) {
                    cancellationToken.cancel()
                }
            }
        }

        when:
        collection.find(new Document()).forEach(cancelBlock, cancellationToken).get()

        then:
        cancelBlock.iterations == 2
    }
}
