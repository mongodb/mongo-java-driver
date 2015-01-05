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
import com.mongodb.ClusterFixture
import com.mongodb.ExplainVerbosity
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.binding.ClusterBinding
import com.mongodb.binding.ConnectionSource
import com.mongodb.binding.ReadBinding
import com.mongodb.connection.ClusterId
import com.mongodb.connection.Connection
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.QueryResult
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.connection.ServerVersion
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getCluster
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.loopCursor
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static java.util.concurrent.TimeUnit.MILLISECONDS

class FindOperationSpecification extends OperationFunctionalSpecification {

    def 'should query with default values'() {
        def document = new Document('_id', 1)
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), document);
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())

        when:
        def cursor = findOperation.execute(getBinding())

        then:
        cursor.next()[0] == document
    }

    def 'should apply filter'() {
        given:
        def document = new Document('_id', 1)
        getCollectionHelper().insertDocuments(new DocumentCodec(), document, new Document());
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .filter(new BsonDocument('_id', new BsonInt32(1)))

        when:
        def cursor = findOperation.execute(getBinding())
        def nextBatch = cursor.next()

        then:
        nextBatch.size() == 1
        nextBatch[0] == document
        !cursor.hasNext()
    }

    def 'should apply sort'() {
        given:
        def documents = [new Document('_id', 3), new Document('_id', 1), new Document('_id', 2), new Document('_id', 5),
                         new Document('_id', 4)]
        getCollectionHelper().insertDocuments(new DocumentCodec(), documents);


        when: 'ascending'
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .sort(new BsonDocument('_id', new BsonInt32(1)))
        def cursor = findOperation.execute(getBinding())
        def list = []
        while (cursor.hasNext()) {
            list += cursor.next()
        }

        then:
        list == [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3), new Document('_id', 4), new Document('_id', 5)]
    }

    def 'should apply projection'() {
        given:
        def document = new Document('_id', 1).append('x', 5).append('y', 10)
        getCollectionHelper().insertDocuments(new DocumentCodec(), document, new Document());
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .projection(new BsonDocument('_id', new BsonInt32(0)).append('x', new BsonInt32(1)))

        when:
        def cursor = findOperation.execute(getBinding())
        def nextBatch = cursor.next()

        then:
        nextBatch[0] == new Document('x', 5)
    }

    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from execute'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        operation.execute(getBinding())

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    @Category(Async)
    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from executeAsync'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .maxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        executeAsync(findOperation);

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    def '$max should limit items returned'() {
        given:
        (1..100).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y').append('count', it))
        }
        collectionHelper.createIndex(new BsonDocument('count', new BsonInt32(1)))
        def count = 0;
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$max', new BsonDocument('count', new BsonInt32(11))))

        when:
        findOperation.execute(getBinding()).each {
            it.each {
                count++
            }
        }

        then:
        count == 10
    }

    def '$min should limit items returned'() {
        given:
        (1..100).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y').append('count', it))
        }
        collectionHelper.createIndex(new BsonDocument('count', new BsonInt32(1)))
        def count = 0;
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$min', new BsonDocument('count', new BsonInt32(10))))

        when:
        findOperation.execute(getBinding()).each {
            it.each {
                count++
            }
        }

        then:
        count == 91
    }

    def '$maxScan should limit items returned'() {
        given:
        (1..100).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y'))
        }
        def count = 0;
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$maxScan', new BsonInt32(34)))

        when:
        findOperation.execute(getBinding()).each {
            it.each {
                count++
            }
        }

        then:
        count == 34
    }

    def '$returnKey should only return the field that was in an index used to perform the find'() {
        given:
        (1..13).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', it))
        }
        collectionHelper.createIndex(new BsonDocument('x', new BsonInt32(1)))

        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .filter(new BsonDocument('x', new BsonInt32(7)))
                .modifiers(new BsonDocument('$returnKey', BsonBoolean.TRUE))

        when:
        def cursor = findOperation.execute(getBinding())

        then:
        def batch = cursor.next()
        batch[0].keySet().size() == 1
        batch[0]['x'] == 7
    }

    def '$showDiskLoc should return disk locations'() {
        given:
        (1..100).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y'))
        }
        def found = true;
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .modifiers(new BsonDocument('$showDiskLoc', BsonBoolean.TRUE))

        when:
        findOperation.execute(getBinding()).each {
            found &= it['$diskLoc'] != null
        }

        then:
        found
    }

    @IgnoreIf({ !ClusterFixture.isDiscoverableReplicaSet() })
    def 'should read from a secondary'() {
        collectionHelper.insertDocuments(new DocumentCodec(), new Document())
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
        def binding = new ClusterBinding(getCluster(), ReadPreference.secondary())

        expect:
        findOperation.execute(binding) != null // if it didn't throw, the query was executed
    }

    @Category([Async, Slow])
    @IgnoreIf({ isSharded() })
    def 'should iterate asynchronously'() {
        given:
        (1..500).each {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', it))
        }
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())

        when:
        def count = 0;
        loopCursor(findOperation, new Block<Document>() {
            @Override
            void apply(final Document value) {
                count++
            }
        });

        then:
        count == 500
    }

    def 'should call query on Connection with correct arguments for an explain'() {
        given:
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())
                .limit(20)
                .batchSize(2)
                .projection(new BsonDocument('x', new BsonInt32(1)))
                .sort(new BsonDocument('y', new BsonInt32(-1)))
                .filter(new BsonDocument('z', new BsonString('val')))
        def binding = Stub(ReadBinding)
        def source = Stub(ConnectionSource)
        def connection = Mock(Connection)
        binding.readPreference >> ReadPreference.primary()
        binding.readConnectionSource >> source
        source.connection >> connection
        source.retain() >> source

        when:
        findOperation.asExplainableOperation(ExplainVerbosity.QUERY_PLANNER).execute(binding)

        then:
        _ * connection.description >> new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                                                                new ServerVersion(2, 6), ServerType.STANDALONE, 1000, 100000, 100000)

        1 * connection.query(getNamespace(),
                             new BsonDocument('$query', findOperation.filter).append('$explain', BsonBoolean.TRUE)
                                                                             .append('$orderby', findOperation.sort),
                             findOperation.projection, -20, 0, false, false, false, false, false, false, _) >>
        new QueryResult(getNamespace(), [new BsonDocument('n', new BsonInt32(1))], 0, new ServerAddress())

        1 * connection.release()
    }

    def 'should explain'() {
        given:
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())

        when:
        BsonDocument result = findOperation.asExplainableOperation(ExplainVerbosity.QUERY_PLANNER).execute(getBinding())

        then:
        result
    }

    @Category(Async)
    def 'should explain asynchronously'() {
        given:
        def findOperation = new FindOperation<Document>(getNamespace(), new DocumentCodec())

        when:
        BsonDocument result = executeAsync(findOperation.asExplainableOperationAsync(ExplainVerbosity.QUERY_PLANNER))

        then:
        result
    }
}
