/*
 * Copyright 2015-2016 MongoDB, Inc.
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

package com.mongodb

import com.mongodb.client.model.Collation
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodec
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.CodecConfigurationException
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static org.bson.codecs.configuration.CodecRegistries.fromProviders
import static spock.util.matcher.HamcrestSupport.expect

class AggregateIterableSpecification extends Specification {

    def namespace = new MongoNamespace('db', 'coll')
    def codecRegistry = fromProviders([new ValueCodecProvider(), new DocumentCodecProvider(), new BsonValueCodecProvider()])
    def readPreference = secondary()
    def readConcern = ReadConcern.DEFAULT
    def writeConcern = WriteConcern.MAJORITY
    def collation = Collation.builder().locale('en').build()

    def 'should build the expected AggregationOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null, null, null, null]);
        def pipeline = [new Document('$match', 1)]
        def aggregationIterable = new AggregateIterableImpl(namespace, Document, Document, codecRegistry, readPreference,
                                                            readConcern, writeConcern, executor, pipeline, collation)

        when: 'default input should be as expected'
        aggregationIterable.iterator()

        def operation = executor.getReadOperation() as AggregateOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace,
                [new BsonDocument('$match', new BsonInt32(1))], new DocumentCodec()).collation(collation));
        readPreference == secondary()

        when: 'overriding initial options'
        aggregationIterable.maxTime(999, MILLISECONDS).useCursor(true).iterator()

        operation = executor.getReadOperation() as AggregateOperation<Document>

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateOperation<Document>(namespace,
                [new BsonDocument('$match', new BsonInt32(1))], new DocumentCodec())
                .collation(collation).maxTime(999, MILLISECONDS).useCursor(true))
    }

    def 'should build the expected AggregateToCollectionOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null, null]);
        def collectionName = 'collectionName'
        def collectionNamespace = new MongoNamespace(namespace.getDatabaseName(), collectionName)
        def pipeline = [new Document('$match', 1), new Document('$out', collectionName)]

        when: 'aggregation includes $out'
        new AggregateIterableImpl(namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                                  pipeline, collation)
                .batchSize(99)
                .maxTime(999, MILLISECONDS)
                .allowDiskUse(true)
                .useCursor(true).iterator()

        def operation = executor.getWriteOperation() as AggregateToCollectionOperation

        then: 'should use the overrides'
        expect operation, isTheSameAs(new AggregateToCollectionOperation(namespace,
                [new BsonDocument('$match', new BsonInt32(1)), new BsonDocument('$out', new BsonString(collectionName))], writeConcern)
                .maxTime(999, MILLISECONDS).allowDiskUse(true).collation(collation))

        when: 'the subsequent read should have the batchSize set'
        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the correct settings'
        operation.getNamespace() == collectionNamespace
        operation.getBatchSize() == 99
    }

    def 'should handle exceptions correctly'() {
        given:
        def codecRegistry = fromProviders([new ValueCodecProvider(), new BsonValueCodecProvider()])
        def executor = new TestOperationExecutor([new MongoException('failure')])
        def pipeline = [new BsonDocument('$match', new BsonInt32(1))]
        def aggregationIterable = new AggregateIterableImpl(namespace, BsonDocument, BsonDocument, codecRegistry,
                                                            readPreference, readConcern, writeConcern, executor, pipeline, collation)

        when: 'The operation fails with an exception'
        aggregationIterable.iterator()

        then: 'the future should handle the exception'
        thrown(MongoException)

        when: 'a codec is missing'
        new AggregateIterableImpl(namespace, Document, Document, codecRegistry, readPreference, readConcern, writeConcern, executor,
                                  pipeline, collation).iterator()

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
        def mongoIterable = new AggregateIterableImpl(namespace, Document, Document, codecRegistry, readPreference,
                readConcern, writeConcern, executor, [new Document('$match', 1)], collation)

        when:
        def results = mongoIterable.first()

        then:
        results == cannedResults[0]

        when:
        def count = 0
        mongoIterable.forEach(new Block<Document>() {
            @Override
            void apply(Document document) {
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

}
