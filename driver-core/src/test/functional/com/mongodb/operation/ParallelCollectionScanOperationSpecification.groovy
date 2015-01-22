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
import category.Slow
import com.mongodb.Block
import com.mongodb.OperationFunctionalSpecification
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import java.util.concurrent.ConcurrentHashMap

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.loopCursor
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static org.junit.Assert.assertTrue

@IgnoreIf({ isSharded() || !serverVersionAtLeast(asList(2, 6, 0)) })
@Category(Slow)
class ParallelCollectionScanOperationSpecification extends OperationFunctionalSpecification {
    Map<Integer, Boolean> ids = [] as ConcurrentHashMap

    def 'setup'() {
        (1..2000).each {
            ids.put(it, true)
            getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', it))
        }
    }

    def 'should visit all documents'() {
        when:
        def cursors = new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec())
                .batchSize(500).execute(getBinding())

        then:
        cursors.size() <= 3

        when:
        cursors.each { batchCursor -> batchCursor.each { cursor -> cursor.each { doc -> ids.remove(doc.getInteger('_id')) } } }

        then:
        ids.isEmpty()
    }

    @Category(Async)
    def 'should visit all documents asynchronously'() {
        when:
        def cursors = executeAsync(new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec()).batchSize(500))

        then:
        cursors.size() <= 3

        when:
        loopCursor(cursors, new Block<Document>() {
            @Override
            void apply(final Document document) {
                assertTrue(ids.remove((Integer) document.get('_id')))
            }
        })

        then:
        ids.isEmpty()
    }

}
