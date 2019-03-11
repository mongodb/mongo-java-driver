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

package com.mongodb.client.internal


import com.mongodb.Function
import com.mongodb.MongoException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.WriteConcern
import com.mongodb.client.ClientSession
import com.mongodb.client.model.Collation
import com.mongodb.client.model.MapReduceAction
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.FindOperation
import com.mongodb.operation.MapReduceToCollectionOperation
import com.mongodb.operation.MapReduceWithInlineResultsOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import java.util.function.Consumer

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class MapReduceIterableSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
    def readPreference = secondary()
    def readConcern = ReadConcern.MAJORITY
    def writeConcern = WriteConcern.MAJORITY
    def collation = Collation.builder().locale('en').build()

    def 'should build the expected MapReduceWithInlineResultsOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def mapReduceIterable = new MapReduceIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, 'map', 'reduce')

        when: 'default input should be as expected'
        mapReduceIterable.iterator()

        def operation = (executor.getReadOperation() as MapReduceIterableImpl.WrappedMapReduceReadOperation<Document>).getOperation()
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new MapReduceWithInlineResultsOperation<Document>(namespace, new BsonJavaScript('map'),
                new BsonJavaScript('reduce'), new DocumentCodec())
                .verbose(true))
        readPreference == secondary()

        when: 'overriding initial options'
        mapReduceIterable.filter(new Document('filter', 1))
                .finalizeFunction('finalize')
                .limit(999)
                .maxTime(999, MILLISECONDS)
                .scope(new Document('scope', 1))
                .sort(new Document('sort', 1))
                .verbose(false)
                .collation(collation)
                .iterator()

        operation = (executor.getReadOperation() as MapReduceIterableImpl.WrappedMapReduceReadOperation<Document>).getOperation()

        then: 'should use the overrides'
        expect operation, isTheSameAs(new MapReduceWithInlineResultsOperation<Document>(namespace, new BsonJavaScript('map'),
                new BsonJavaScript('reduce'), new DocumentCodec())
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript('finalize'))
                .limit(999)
                .maxTime(999, MILLISECONDS)
                .scope(new BsonDocument('scope', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .verbose(false)
                .collation(collation)
        )
    }

    def 'should build the expected MapReduceToCollectionOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null, null]);

        when: 'mapReduce to a collection'
        def collectionNamespace = new MongoNamespace('dbName', 'collName')
        def mapReduceIterable = new MapReduceIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern,
                writeConcern, executor, 'map', 'reduce')
                .collectionName(collectionNamespace.getCollectionName())
                .databaseName(collectionNamespace.getDatabaseName())
                .filter(new Document('filter', 1))
                .finalizeFunction('finalize')
                .limit(999)
                .maxTime(999, MILLISECONDS)
                .scope(new Document('scope', 1))
                .sort(new Document('sort', 1))
                .verbose(false)
                .batchSize(99)
                .nonAtomic(true)
                .action(MapReduceAction.MERGE)
                .sharded(true)
                .jsMode(true)
                .bypassDocumentValidation(true)
                .collation(collation)
        mapReduceIterable.iterator()

        def operation = executor.getWriteOperation() as MapReduceToCollectionOperation
        def expectedOperation = new MapReduceToCollectionOperation(namespace, new BsonJavaScript('map'),
                new BsonJavaScript('reduce'), 'collName', writeConcern)
                .databaseName(collectionNamespace.getDatabaseName())
                .filter(new BsonDocument('filter', new BsonInt32(1)))
                .finalizeFunction(new BsonJavaScript('finalize'))
                .limit(999)
                .maxTime(999, MILLISECONDS)
                .scope(new BsonDocument('scope', new BsonInt32(1)))
                .sort(new BsonDocument('sort', new BsonInt32(1)))
                .verbose(false)
                .nonAtomic(true)
                .action(MapReduceAction.MERGE.getValue())
                .jsMode(true)
                .sharded(true)
                .bypassDocumentValidation(true)
                .collation(collation)

        then: 'should use the overrides'
        expect operation, isTheSameAs(expectedOperation)

        when: 'the subsequent read should have the batchSize set'
        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the correct settings'
        operation.getNamespace() == collectionNamespace
        operation.getBatchSize() == 99
        operation.getCollation() == collation

        when: 'toCollection should work as expected'
        mapReduceIterable.toCollection()

        operation = executor.getWriteOperation() as MapReduceToCollectionOperation

        then:
        expect operation, isTheSameAs(expectedOperation)
    }

    def 'should use ClientSession for MapReduceWithInlineResultsOperation'() {
        given:
        def batchCursor = Stub(BatchCursor) {
            _ * hasNext() >> { false }
        }
        def executor = new TestOperationExecutor([batchCursor, batchCursor]);
        def mapReduceIterable = new MapReduceIterableImpl(clientSession, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, 'map', 'reduce')

        when:
        mapReduceIterable.first()

        then:
        executor.getClientSession() == clientSession

        when:
        mapReduceIterable.iterator()

        then:
        executor.getClientSession() == clientSession

        where:
        clientSession << [null, Stub(ClientSession)]
    }

    def 'should use ClientSession for MapReduceToCollectionOperation'() {
        given:
        def batchCursor = Stub(BatchCursor) {
            _ * hasNext() >> { false }
        }
        def executor = new TestOperationExecutor([null, batchCursor, null, batchCursor, null]);
        def mapReduceIterable = new MapReduceIterableImpl(clientSession, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, 'map', 'reduce')
                .collectionName('collName')

        when:
        mapReduceIterable.first()

        then:
        executor.getClientSession() == clientSession
        executor.getClientSession() == clientSession

        when:
        mapReduceIterable.iterator()

        then:
        executor.getClientSession() == clientSession
        executor.getClientSession() == clientSession

        when:
        mapReduceIterable.toCollection()

        then:
        executor.getClientSession() == clientSession

        where:
        clientSession << [null, Stub(ClientSession)]
    }


    def 'should handle exceptions correctly'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def mapReduceIterable = new MapReduceIterableImpl(null, namespace, BsonDocument, BsonDocument, codecRegistry,
                readPreference, readConcern, writeConcern, executor, 'map', 'reduce')


        when: 'The operation fails with an exception'
        mapReduceIterable.iterator()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'toCollection should throw IllegalStateException if its inline'
        mapReduceIterable.toCollection()

        then:
        thrown(IllegalStateException)

        when: 'a codec is missing'
        new MapReduceIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                'map', 'reduce').iterator()

        then:
        thrown(CodecConfigurationException)
    }


    def 'should follow the MongoIterable interface as expected'() {
        given:
        def cannedResults = [new Document('_id', 1), new Document('_id', 2), new Document('_id', 3)]
        def cursor = {
            Stub(BatchCursor) {
                def count = 0
                def results;
                def getResult = {
                    count++
                    results = count == 1 ? cannedResults : null
                    results
                }
                next() >> {
                    getResult()
                }
                hasNext() >> {
                    count == 0
                }
            }
        }
        def executor = new TestOperationExecutor([cursor(), cursor(), cursor(), cursor()]);
        def mongoIterable = new MapReduceIterableImpl(null, namespace, BsonDocument, BsonDocument, codecRegistry, readPreference,
                readConcern, writeConcern, executor, 'map', 'reduce')

        when:
        def results = mongoIterable.first()

        then:
        results == cannedResults[0]

        when:
        def count = 0
        mongoIterable.forEach(new Consumer<Document>() {
            @Override
            void accept(Document document) {
                count++
            }
        })

        then:
        count == 3

        when:
        def target = []
        mongoIterable.into(target)

        then:
        target == cannedResults

        when:
        target = []
        mongoIterable.map(new Function<Document, Integer>() {
            @Override
            Integer apply(Document document) {
                document.getInteger('_id')
            }
        }).into(target)

        then:
        target == [1, 2, 3]
    }

    def 'should get and set batchSize as expected'() {
        when:
        def batchSize = 5
        def mongoIterable = new MapReduceIterableImpl(null, namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, Stub(OperationExecutor), 'map', 'reduce')

        then:
        mongoIterable.getBatchSize() == null

        when:
        mongoIterable.batchSize(batchSize)

        then:
        mongoIterable.getBatchSize() == batchSize
    }

}
