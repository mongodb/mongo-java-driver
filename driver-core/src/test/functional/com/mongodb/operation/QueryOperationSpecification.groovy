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
import com.mongodb.Block
import com.mongodb.ClusterFixture
import com.mongodb.MongoExecutionTimeoutException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.binding.ClusterBinding
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.junit.experimental.categories.Category
import org.mongodb.Document
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.disableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.enableMaxTimeFailPoint
import static com.mongodb.ClusterFixture.getAsyncBinding
import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.isSharded
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.CursorFlag.EXHAUST
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class QueryOperationSpecification extends OperationFunctionalSpecification {

    def 'should query with default values'() {
        def document = new Document('_id', 1)
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), document);
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())

        when:
        def cursor = queryOperation.execute(getBinding())

        then:
        cursor.next() == document
    }

    def 'should apply criteria'() {
        given:
        def document = new Document('_id', 1)
        getCollectionHelper().insertDocuments(new DocumentCodec(), document, new Document());
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setCriteria(new BsonDocument('_id', new BsonInt32(1)))

        when:
        def cursor = queryOperation.execute(getBinding())

        then:
        cursor.next() == document
        !cursor.hasNext()
    }

    def 'should apply sort'() {
        given:
        def documents = [new Document('_id', 3), new Document('_id', 1), new Document('_id', 2), new Document('_id', 5),
                         new Document('_id', 4)]
        getCollectionHelper().insertDocuments(new DocumentCodec(), documents);


        when: 'ascending'
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setSort(new BsonDocument('_id', new BsonInt32(1)))
        def cursor = queryOperation.execute(getBinding())
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
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setProjection(new BsonDocument('_id', new BsonInt32(0)).append('x', new BsonInt32(1)))

        when:
        def cursor = queryOperation.execute(getBinding())

        then:
        cursor.next() == new Document('x', 5)
    }

    @IgnoreIf({ isSharded() || !serverVersionAtLeast([2, 6, 0]) })
    def 'should throw execution timeout exception from execute'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document())
        def operation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        operation.setMaxTime(1000, MILLISECONDS)

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
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setMaxTime(1000, MILLISECONDS)

        enableMaxTimeFailPoint()

        when:
        queryOperation.executeAsync(getAsyncBinding()).get();

        then:
        thrown(MongoExecutionTimeoutException)

        cleanup:
        disableMaxTimeFailPoint()
    }

    def '$max should limit items returned'() {
        given:
        for ( i in 1..100) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y').append('count', i))
        }
        collectionHelper.createIndexes([Index.builder().addKey('count').build()])
        def count = 0;
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setModifiers(new BsonDocument('$max', new BsonDocument('count', new BsonInt32(11))))

        when:
        queryOperation.execute(getBinding()).each {
            count++
        }

        then:
        count == 10
    }

    def '$min should limit items returned'() {
        given:
        for (i in 1..100) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y').append('count', i))
        }
        collectionHelper.createIndexes([Index.builder().addKey('count').build()])
        def count = 0;
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setModifiers(new BsonDocument('$min', new BsonDocument('count', new BsonInt32(10))))

        when:
        queryOperation.execute(getBinding()).each {
            count++
        }

        then:
        count == 91
    }

    def '$maxScan should limit items returned'() {
        given:
        for (i in 1..100) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y'))
        }
        def count = 0;
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setModifiers(new BsonDocument('$maxScan', new BsonInt32(34)))

        when:
        queryOperation.execute(getBinding()).each {
            count++
        }

        then:
        count == 34
    }

    def '$returnKey should only return the field that was in an index used to perform the find'() {
        given:
        for (i in 1..13) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', i))
        }
        collectionHelper.createIndexes([Index.builder().addKey('x').build()])

        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setCriteria(new BsonDocument('x', new BsonInt32(7)))
        queryOperation.setModifiers(new BsonDocument('$returnKey', BsonBoolean.TRUE))

        when:
        def cursor = queryOperation.execute(getBinding())

        then:
        def foundItem = cursor.next()
        foundItem.keySet().size() == 1
        foundItem['x'] == 7
    }

    def '$showDiskLoc should return disk locations'() {
        given:
        for (i in 1..100) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('x', 'y'))
        }
        def found = true;
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setModifiers(new BsonDocument('$showDiskLoc', BsonBoolean.TRUE))

        when:
        queryOperation.execute(getBinding()).each {
            found &= it['$diskLoc'] != null
        }

        then:
        found
    }

    @IgnoreIf({ !ClusterFixture.isDiscoverableReplicaSet() })
    def 'should read from a secondary'() {
        collectionHelper.insertDocuments(new DocumentCodec(), new Document())
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        def binding = new ClusterBinding(getCluster(), ReadPreference.secondary(), 1, SECONDS)

        expect:
        queryOperation.execute(binding) != null // if it didn't throw, the query was executed
    }

    @IgnoreIf({ isSharded() })
    def 'should exhaust'() {
        for (i in 1..500) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', i))
        }
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setCursorFlags(EnumSet.of(EXHAUST))

        when:
        def count = 0;

        def cursor = queryOperation.execute(getBinding())
        try {
            while (cursor.hasNext()) {
                cursor.next();
                count++;
            }
        } finally {
            cursor.close()
        }

        then:
        count == 500
    }

    @Category(Async)
    @IgnoreIf({ isSharded() })
    def 'should iterate asynchronously'() {
        given:
        for (i in 1..500) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', i))
        }
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())

        when:
        def count = 0;

        def cursor = queryOperation.executeAsync(getAsyncBinding())
        cursor.get().forEach(new Block<Document>() {
            @Override
            void apply(final Document document) {
                count++;
            }
        }).get()

        then:
        count == 500
    }

    @Category(Async)
    @IgnoreIf({ isSharded() })
    def 'should exhaust asynchronously'() {
        for (i in 1..500) {
            collectionHelper.insertDocuments(new DocumentCodec(), new Document('_id', i))
        }
        def queryOperation = new QueryOperation<Document>(getNamespace(), new DocumentCodec())
        queryOperation.setCursorFlags(EnumSet.of(EXHAUST))

        when:
        def count = 0;

        def cursor = queryOperation.executeAsync(getAsyncBinding())
        cursor.get().forEach(new Block<Document>() {
            @Override
            void apply(final Document document) {
                count++;
            }
        }).get()

        then:
        count == 500
    }
}
