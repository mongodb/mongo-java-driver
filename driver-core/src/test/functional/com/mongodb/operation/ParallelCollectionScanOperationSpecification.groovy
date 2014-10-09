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
import com.mongodb.MongoCursor
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.async.MongoAsyncCursor
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.Arrays.asList
import static org.junit.Assert.assertTrue

@IgnoreIf({ isSharded() || !serverVersionAtLeast(asList(2, 6, 0)) })
@Category(Slow)
class ParallelCollectionScanOperationSpecification extends OperationFunctionalSpecification {
    Set<Integer> ids = [] as Set

    def 'setup'() {
        (1..2000).each {
            ids.add(it)
            getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', it))
        }
    }

    def 'should visit all documents'() {
        when:
        List<MongoCursor<Document>> cursors = new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec())
                .batchSize(500).execute(getBinding())

        then:
        cursors.size() <= 3

        when:
        for (MongoCursor<Document> cursor : cursors) {
            while (cursor.hasNext()) {
                Integer id = (Integer) cursor.next().get('_id')
                assertTrue(ids.remove(id))
            }
        }

        then:
        ids.isEmpty()
    }

    @Category(Async)
    def 'should visit all documents asynchronously'() {
        when:
        List<MongoAsyncCursor<Document>> cursors = new ParallelCollectionScanOperation<Document>(getNamespace(), 3, new DocumentCodec())
                .batchSize(500).executeAsync(getAsyncBinding()).get()

        then:
        cursors.size() <= 3

        when:
        for (MongoAsyncCursor<Document> cursor : cursors) {
            cursor.forEach(new Block<Document>() {
                @Override
                void apply(final Document document) {
                    Integer id = (Integer) document.get('_id')
                    assertTrue(ids.remove(id))
                }
            }).get()
        }

        then:
        ids.isEmpty()
    }
}