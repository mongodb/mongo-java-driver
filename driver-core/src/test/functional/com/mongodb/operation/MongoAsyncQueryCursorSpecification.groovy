/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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
import com.mongodb.MongoInternalException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.async.MongoFuture
import com.mongodb.binding.AsyncClusterBinding
import com.mongodb.binding.AsyncConnectionSource
import com.mongodb.binding.AsyncReadBinding
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.connection.Connection
import com.mongodb.connection.QueryResult
import org.bson.BsonDocumentWrapper
import org.bson.BsonTimestamp
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.Shared

import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getAsyncCluster
import static com.mongodb.ReadPreference.primary
import static java.util.concurrent.TimeUnit.SECONDS

@Category([Async, Slow])
class MongoAsyncQueryCursorSpecification extends OperationFunctionalSpecification {

    @Shared
    List<Document> documentList

    private List<Document> documentResultList
    private AsyncReadBinding binding
    private AsyncConnectionSource source

    def setup() {
        documentList = []
        documentResultList = []
        (1..1000).each {
            documentList.add(new Document('_id', it))

        }
        getCollectionHelper().insertDocuments(new DocumentCodec(), *documentList)

        binding = new AsyncClusterBinding(getAsyncCluster(), primary(), 1, SECONDS)
        source = binding.getReadConnectionSource().get()
    }

    def cleanup() {
        binding.release()
    }

    def 'Cursor should iterate all contents'() {
        given:
        QueryResult<Document> firstBatch = executeQuery()

        when:
        new MongoAsyncQueryCursor<Document>(getNamespace(),
                                            firstBatch, 0, 2, new DocumentCodec(),
                                            source)
                .forEach(new TestBlock()).get()

        then:
        documentList == documentResultList
    }

    def 'Cursor should support limit'() {
        given:
        QueryResult<Document> firstBatch = executeQuery()

        when:
        new MongoAsyncQueryCursor<Document>(getNamespace(),
                                            firstBatch, 100, 0, new DocumentCodec(),
                                            source)
                .forEach(new TestBlock()).get()

        then:
        documentResultList == documentList[0..99]
    }

    @Category(Slow)
    def 'Cursor should be tailable'() {
        setup:
        AsyncConnectionSource source = getAsyncBinding().getReadConnectionSource().get()
        Connection connection = source.getConnection().get()
        getCollectionHelper().create(getCollectionName(), new CreateCollectionOptions().capped(true).sizeInBytes(1000))
        def timestamp = new BsonTimestamp(5, 0)
        getCollectionHelper().insertDocuments(new DocumentCodec(), [_id: 1, ts: timestamp] as Document)

        QueryResult<Document> firstBatch = executeTailableQuery([ts: ['$gte': timestamp] as Document] as Document, 2, connection)
        TestBlock block = new TestBlock(2)

        when:
        MongoFuture<Void> future = new MongoAsyncQueryCursor<Document>(getNamespace(),
                                                                       firstBatch, 5, 2, new DocumentCodec(),
                                                                       source).forEach(block)
        then:
        block.getIterations() == 1

        when:
        getCollectionHelper().insertDocuments(new DocumentCodec(), [_id: 2, ts: new BsonTimestamp(1, 0)] as Document)
        getCollectionHelper().insertDocuments(new DocumentCodec(), [_id: 3, ts: new BsonTimestamp(6, 0)] as Document)
        getCollectionHelper().insertDocuments(new DocumentCodec(), [_id: 4, ts: new BsonTimestamp(8, 0)] as Document)
        future.get()

        then:
        thrown(MongoInternalException)
        block.getIterations() == 2
        documentResultList*.get('_id') == [1, 3]

        cleanup:
        connection.release()
        source.release()
    }

    private static Document getOrderedByIdQuery() {
        new Document('$query', new Document()).append('$orderby', new Document('_id', 1))
    }

    private QueryResult<Document> executeQuery() {
        executeQuery(getOrderedByIdQuery(), 0, false, false)
    }

    private QueryResult<Document> executeTailableQuery(Document query, int numberToReturn, Connection connection) {
        executeQuery(query, numberToReturn, true, true, connection)
    }

    private QueryResult<Document> executeQuery(Document query, int numberToReturn, boolean tailable, boolean awaitData) {
        Connection connection = source.getConnection().get()
        try {
            executeQuery(query, numberToReturn, tailable, awaitData, connection)
        } finally {
            connection.release()
        }
    }

    private QueryResult<Document> executeQuery(Document query, int numberToReturn, boolean tailable, boolean awaitData,
                                               Connection connection) {
        connection.query(getNamespace(), new BsonDocumentWrapper<Document>(query, new DocumentCodec()), null, numberToReturn, 0,
                         false, tailable, awaitData, false, false, false, new DocumentCodec())
    }

    private final class TestBlock implements Block<Document> {
        private final int count
        private int iterations

        private TestBlock() {
            this(Integer.MAX_VALUE)
        }

        private TestBlock(final int count) {
            this.count = count
        }

        @SuppressWarnings(['ThrowRuntimeException'])
        @Override
        void apply(final Document document) {
            if (iterations >= count) {
                throw new RuntimeException('Discard the rest')
            }
            iterations++
            documentResultList.add(document)
        }

        int getIterations() {
            iterations
        }
    }
}

