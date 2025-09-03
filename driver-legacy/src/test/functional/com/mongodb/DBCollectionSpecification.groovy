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

package com.mongodb

import com.mongodb.client.internal.TestOperationExecutor
import com.mongodb.client.model.Collation
import com.mongodb.client.model.CollationAlternate
import com.mongodb.client.model.CollationCaseFirst
import com.mongodb.client.model.CollationMaxVariable
import com.mongodb.client.model.CollationStrength
import com.mongodb.client.model.DBCollectionCountOptions
import com.mongodb.client.model.DBCollectionDistinctOptions
import com.mongodb.client.model.DBCollectionFindAndModifyOptions
import com.mongodb.client.model.DBCollectionFindOptions
import com.mongodb.client.model.DBCollectionRemoveOptions
import com.mongodb.client.model.DBCollectionUpdateOptions
import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.IndexRequest
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.bulk.UpdateRequest
import com.mongodb.internal.operation.AggregateOperation
import com.mongodb.internal.operation.AggregateToCollectionOperation
import com.mongodb.internal.operation.BatchCursor
import com.mongodb.internal.operation.CountOperation
import com.mongodb.internal.operation.CreateIndexesOperation
import com.mongodb.internal.operation.DistinctOperation
import com.mongodb.internal.operation.FindAndDeleteOperation
import com.mongodb.internal.operation.FindAndReplaceOperation
import com.mongodb.internal.operation.FindAndUpdateOperation
import com.mongodb.internal.operation.FindOperation
import com.mongodb.internal.operation.MapReduceBatchCursor
import com.mongodb.internal.operation.MapReduceStatistics
import com.mongodb.internal.operation.MapReduceToCollectionOperation
import com.mongodb.internal.operation.MapReduceWithInlineResultsOperation
import com.mongodb.internal.operation.MixedBulkWriteOperation
import org.bson.BsonBinary
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.UuidRepresentation
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodec
import org.bson.codecs.UuidCodec
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static Fixture.getMongoClient
import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForDelete
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForUpdate
import static java.util.Arrays.asList
import static org.bson.codecs.configuration.CodecRegistries.fromCodecs
import static spock.util.matcher.HamcrestSupport.expect

class DBCollectionSpecification extends Specification {

    private static final DEFAULT_DBOBJECT_CODEC_FACTORY = new DBObjectCodec(MongoClient.getDefaultCodecRegistry(),
            DBObjectCodec.getDefaultBsonTypeClassMap(),
            new DBCollectionObjectFactory())

    def 'should throw IllegalArgumentException if name is invalid'() {
        when:
        new DB(getMongoClient(), 'myDatabase', new TestOperationExecutor([])).getCollection('')

        then:
        thrown(IllegalArgumentException)
    }

    def 'should use MongoClient CodecRegistry'() {
        given:
        def mongoClient = Stub(MongoClient) {
            getCodecRegistry() >> fromCodecs(new UuidCodec(UuidRepresentation.STANDARD))
            getReadConcern() >> ReadConcern.DEFAULT
            getWriteConcern() >> WriteConcern.ACKNOWLEDGED
            getMongoClientOptions() >> MongoClientOptions.builder().build()
        }
        def executor = new TestOperationExecutor([WriteConcernResult.unacknowledged()])
        def db = new DB(mongoClient, 'myDatabase', executor)
        def collection = db.getCollection('test')
        def uuid = UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')

        when:
        collection.insert(new BasicDBObject('_id', uuid))
        def operation = executor.writeOperation as LegacyMixedBulkWriteOperation

        then:
        (operation.writeRequests[0] as InsertRequest).document.getBinary('_id') == new BsonBinary(uuid, UuidRepresentation.STANDARD)
    }

    def 'should get and set read concern'() {
        when:
        def db = new DB(getMongoClient(), 'myDatabase', new TestOperationExecutor([]))
        db.setReadConcern(ReadConcern.MAJORITY)
        def collection = db.getCollection('test')

        then:
        collection.readConcern == ReadConcern.MAJORITY

        when:
        collection.setReadConcern(ReadConcern.LOCAL)

        then:
        collection.readConcern == ReadConcern.LOCAL

        when:
        collection.setReadConcern(null)

        then:
        collection.readConcern == ReadConcern.MAJORITY
    }

    def 'should use CreateIndexOperation properly'() {
        given:
        def executor = new TestOperationExecutor([null, null, null])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def keys = new BasicDBObject('a', 1)

        when:
        collection.createIndex(keys)
        def request = (executor.getWriteOperation() as CreateIndexesOperation).requests[0]

        then:
        expect request, isTheSameAs(new IndexRequest(new BsonDocument('a', new BsonInt32(1))))

        when:
        def storageEngine = '{ wiredTiger: { configString: "block_compressor=zlib" }}'
        def partialFilterExpression = '{ a: { $gte: 10 } }'
        def collation = Collation.builder()
                .locale('en')
                .caseLevel(true)
                .collationCaseFirst(CollationCaseFirst.OFF)
                .collationStrength(CollationStrength.IDENTICAL)
                .numericOrdering(true)
                .collationAlternate(CollationAlternate.SHIFTED)
                .collationMaxVariable(CollationMaxVariable.SPACE)
                .backwards(true)
                .build()
        collection.createIndex(keys, new BasicDBObject(['background': true, 'unique': true, 'sparse': true, 'name': 'aIndex',
                                                      'expireAfterSeconds': 100, 'v': 1, 'weights': new BasicDBObject(['a': 1000]),
                                                      'default_language': 'es', 'language_override': 'language', 'textIndexVersion': 1,
                                                      '2dsphereIndexVersion': 1, 'bits': 1, 'min': new Double(-180.0),
                                                      'max'          : new Double(180.0), 'dropDups': true,
                                                      'storageEngine': BasicDBObject.parse(storageEngine),
                                                      'partialFilterExpression':  BasicDBObject.parse(partialFilterExpression),
                                                      'collation': BasicDBObject.parse(collation.asDocument().toJson())]))

        request = (executor.getWriteOperation() as CreateIndexesOperation).requests[0]

        then:
        expect request, isTheSameAs(new IndexRequest(new BsonDocument('a', new BsonInt32(1)))
                .background(true)
                .unique(true)
                .sparse(true)
                .name('aIndex')
                .expireAfter(100, TimeUnit.SECONDS)
                .version(1)
                .weights(new BsonDocument('a', new BsonInt32(1000)))
                .defaultLanguage('es')
                .languageOverride('language')
                .textVersion(1)
                .sphereVersion(1)
                .bits(1)
                .min(-180.0)
                .max(180.0)
                .dropDups(true)
                .storageEngine(BsonDocument.parse(storageEngine))
                .partialFilterExpression(BsonDocument.parse(partialFilterExpression))
                .collation(collation))
    }

    def 'should support boolean index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('sparse', value)

        when:
        collection.createIndex(new BasicDBObject('y', 1), options)

        then:
        def operation = executor.getWriteOperation() as CreateIndexesOperation
        operation.requests[0].sparse == expectedValue

        where:
        value | expectedValue
        0     | false
        0F    | false
        0D    | false
        1     | true
        -1    | true
        4L    | true
        4.3F  | true
        4.0D  | true
    }

    def 'should support integer index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('expireAfterSeconds', integerValue)

        when:
        collection.createIndex(new BasicDBObject('y', 1), options)

        then:
        def operation = executor.getWriteOperation() as CreateIndexesOperation
        operation.requests[0].getExpireAfter(TimeUnit.SECONDS) == integerValue

        where:
        integerValue << [4, 4L, (double) 4.0]
    }

    def 'should support double index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('max', doubleValue)

        when:
        collection.createIndex(new BasicDBObject('y', '2d'), options)

        then:
        def operation = executor.getWriteOperation() as CreateIndexesOperation
        operation.requests[0].max == doubleValue

        where:
        doubleValue << [4, 4L, (double) 4.0]
    }

    def 'should throw IllegalArgumentException for unsupported option value type'() {
        given:
        def executor = new TestOperationExecutor([null, null])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('sparse', 'true')


        when:
        collection.createIndex(new BasicDBObject('y', '1'), options)

        then:
        thrown(IllegalArgumentException)
    }

    def 'find should create the correct FindOperation'() {
        given:
        def cursor = Stub(BatchCursor) {
            hasNext() >> false
            getServerCursor() >> new ServerCursor(12L, new ServerAddress())
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        collection.find().iterator().hasNext()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .retryReads(true))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.find().iterator().hasNext()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .retryReads(true))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.find(new BasicDBObject(), new DBCollectionFindOptions().collation(collation)).iterator().hasNext()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .collation(collation)
                .retryReads(true))
    }

    def 'findOne should create the correct FindOperation'() {
        given:
        def dbObject = new BasicDBObject('_id', 1)
        def cursor = Stub(BatchCursor) {
            next() >> [dbObject]
            hasNext() >> true
            getServerCursor() >> new ServerCursor(12L, new ServerAddress())
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        collection.findOne()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .limit(-1)
                .retryReads(true))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.findOne()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .limit(-1)
                .retryReads(true))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.findOne(new BasicDBObject(), new DBCollectionFindOptions().collation(collation))

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(),
                collection.getObjectCodec())
                .filter(new BsonDocument())
                .limit(-1)
                .collation(collation)
                .retryReads(true))
    }

    def 'findAndRemove should create the correct FindAndDeleteOperation'() {
        given:
        def query = new BasicDBObject()
        def cannedResult = BasicDBObject.parse('{value: {}}')
        def executor = new TestOperationExecutor([cannedResult, cannedResult, cannedResult])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')

        when:
        collection.findAndRemove(query)

        then:
        expect executor.getWriteOperation(), isTheSameAs(new FindAndDeleteOperation<DBObject>(collection.
                getNamespace(), WriteConcern.ACKNOWLEDGED, retryWrites, collection.getObjectCodec()).filter(new BsonDocument()))
    }

    def 'findAndModify should create the correct FindAndUpdateOperation'() {
        given:
        def query = new BasicDBObject()
        def updateJson = '{$set: {a : 1}}'
        def update = BasicDBObject.parse(updateJson)
        def bsonUpdate = BsonDocument.parse(updateJson)
        def cannedResult = BasicDBObject.parse('{value: {}}')
        def executor = new TestOperationExecutor([cannedResult, cannedResult, cannedResult])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')

        when:
        collection.findAndModify(query, update)

        then:
        expect executor.getWriteOperation(), isTheSameAs(new FindAndUpdateOperation<DBObject>(collection.getNamespace(),
                WriteConcern.ACKNOWLEDGED, retryWrites, collection.getObjectCodec(), bsonUpdate)
                .filter(new BsonDocument()))

        when: // With options
        collection.findAndModify(query, new DBCollectionFindAndModifyOptions().update(update).collation(collation)
                .arrayFilters(dbObjectArrayFilters).writeConcern(WriteConcern.W3))

        then:
        expect executor.getWriteOperation(), isTheSameAs(new FindAndUpdateOperation<DBObject>(collection.getNamespace(), WriteConcern.W3,
                retryWrites, collection.getObjectCodec(), bsonUpdate)
                .filter(new BsonDocument())
                .collation(collation)
                .arrayFilters(bsonDocumentWrapperArrayFilters))

        where:
        dbObjectArrayFilters <<            [null, [], [new BasicDBObject('i.b', 1)]]
        bsonDocumentWrapperArrayFilters << [null, [], [new BsonDocumentWrapper<BasicDBObject>(new BasicDBObject('i.b', 1),
                DEFAULT_DBOBJECT_CODEC_FACTORY)]]
    }

    def 'findAndModify should create the correct FindAndReplaceOperation'() {
        given:
        def query = new BasicDBObject()
        def replacementJson = '{a : 1}'
        def replace = BasicDBObject.parse(replacementJson)
        def bsonReplace = BsonDocument.parse(replacementJson)
        def cannedResult = BasicDBObject.parse('{value: {}}')
        def executor = new TestOperationExecutor([cannedResult, cannedResult, cannedResult])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')

        when:
        collection.findAndModify(query, replace)

        then:
        expect executor.getWriteOperation(), isTheSameAs(new FindAndReplaceOperation<DBObject>(collection.
                getNamespace(), WriteConcern.ACKNOWLEDGED, retryWrites, collection.getObjectCodec(), bsonReplace)
                .filter(new BsonDocument()))

        when: // With options
        collection.findAndModify(query, new DBCollectionFindAndModifyOptions().update(replace).collation(collation)
                .writeConcern(WriteConcern.W3))

        then:
        expect executor.getWriteOperation(), isTheSameAs(new FindAndReplaceOperation<DBObject>(collection.getNamespace(), WriteConcern.W3,
                retryWrites, collection.getObjectCodec(), bsonReplace)
                .filter(new BsonDocument())
                .collation(collation))
    }

    def 'count should create the correct CountOperation'() {
        given:
        def executor = new TestOperationExecutor([42L, 42L, 42L])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        collection.count()

        then:
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument()).retryReads(true))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.count()
        executor.getReadConcern() == ReadConcern.MAJORITY

        then:
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument()).retryReads(true))
        executor.getReadConcern() == ReadConcern.MAJORITY

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.count(new BasicDBObject(), new DBCollectionCountOptions().collation(collation))

        then:
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument()).retryReads(true)
                .collation(collation))
        executor.getReadConcern() == ReadConcern.LOCAL
    }

    def 'distinct should create the correct DistinctOperation'() {
        given:
        def cursor = Stub(BatchCursor) {
            def count = 0
            next() >> {
                count++
                [new BsonInt32(1), new BsonInt32(2)]
            }
            hasNext() >> {
                count == 0
            }
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        def distinctFieldValues = collection.distinct('field1')

        then:
        distinctFieldValues == [1, 2]
        expect executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), 'field1',
                new BsonValueCodec()).filter(new BsonDocument()).retryReads(true))
        executor.getReadConcern() == ReadConcern.DEFAULT

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.distinct('field1')

        then:
        expect executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), 'field1',
                new BsonValueCodec())
                .filter(new BsonDocument()).retryReads(true))
        executor.getReadConcern() == ReadConcern.MAJORITY

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.distinct('field1', new DBCollectionDistinctOptions().collation(collation))

        then:
        expect executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), 'field1',
                new BsonValueCodec()).collation(collation).retryReads(true))
        executor.getReadConcern() == ReadConcern.LOCAL
    }

    def 'mapReduce should create the correct MapReduceInlineResultsOperation'() {
        given:
        def cursor = Stub(MapReduceBatchCursor) {
            next() >> { }
            hasNext() >> false
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        collection.mapReduce('map', 'reduce', null, MapReduceCommand.OutputType.INLINE, new BasicDBObject())

        then:
        expect executor.getReadOperation(), isTheSameAs(
                new MapReduceWithInlineResultsOperation(collection.getNamespace(), new BsonJavaScript('map'),
                        new BsonJavaScript('reduce'), collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument()))
        executor.getReadConcern() == ReadConcern.DEFAULT

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.LOCAL)
        collection.mapReduce('map', 'reduce', null, MapReduceCommand.OutputType.INLINE, new BasicDBObject())

        then:
        expect executor.getReadOperation(), isTheSameAs(
                new MapReduceWithInlineResultsOperation(collection.getNamespace(), new BsonJavaScript('map'),
                        new BsonJavaScript('reduce'), collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument()))
        executor.getReadConcern() == ReadConcern.LOCAL

        when:
        collection.setReadConcern(ReadConcern.MAJORITY)
        def mapReduceCommand = new MapReduceCommand(collection, 'map', 'reduce', null, MapReduceCommand.OutputType.INLINE,
                new BasicDBObject())
        mapReduceCommand.setCollation(collation)
        collection.mapReduce(mapReduceCommand)

        then:
        expect executor.getReadOperation(), isTheSameAs(
                new MapReduceWithInlineResultsOperation(collection.getNamespace(), new BsonJavaScript('map'),
                        new BsonJavaScript('reduce'), collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument())
                        .collation(collation))
        executor.getReadConcern() == ReadConcern.MAJORITY
    }

    def 'mapReduce should create the correct MapReduceToCollectionOperation'() {
        given:
        def stats = Stub(MapReduceStatistics)
        def executor = new TestOperationExecutor([stats, stats, stats])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        collection.mapReduce('map', 'reduce', 'myColl', MapReduceCommand.OutputType.REPLACE, new BasicDBObject())

        then:
        expect executor.getWriteOperation(), isTheSameAs(
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript('map'),
                        new BsonJavaScript('reduce'), 'myColl', collection.getWriteConcern())
                        .verbose(true)
                        .filter(new BsonDocument())
        )

        when: // Inherits from DB
        collection.mapReduce('map', 'reduce', 'myColl', MapReduceCommand.OutputType.REPLACE, new BasicDBObject())

        then:
        expect executor.getWriteOperation(), isTheSameAs(
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript('map'),
                        new BsonJavaScript('reduce'), 'myColl', collection.getWriteConcern())
                        .verbose(true)
                        .filter(new BsonDocument())
        )

        when:
        def mapReduceCommand = new MapReduceCommand(collection, 'map', 'reduce', 'myColl', MapReduceCommand.OutputType.REPLACE,
                new BasicDBObject())
        mapReduceCommand.setCollation(collation)
        collection.mapReduce(mapReduceCommand)

        then:
        expect executor.getWriteOperation(), isTheSameAs(
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript('map'),
                        new BsonJavaScript('reduce'), 'myColl', collection.getWriteConcern())
                        .verbose(true)
                        .filter(new BsonDocument())
                        .collation(collation)
        )
    }

    def 'aggregate should create the correct AggregateOperation'() {
        given:
        def cursor = Stub(MapReduceBatchCursor) {
            next() >> { }
            hasNext() >> false
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')
        def pipeline = [BasicDBObject.parse('{$match: {}}')]
        def bsonPipeline = [BsonDocument.parse('{$match: {}}')]

        when:
        collection.aggregate(pipeline, AggregationOptions.builder().build())

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true))
        executor.getReadConcern() == ReadConcern.DEFAULT

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.aggregate(pipeline, AggregationOptions.builder().build())

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true))
        executor.getReadConcern() == ReadConcern.MAJORITY

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.aggregate(pipeline, AggregationOptions.builder().collation(collation).build())

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).collation(collation).retryReads(true))
        executor.getReadConcern() == ReadConcern.LOCAL
    }

    def 'aggregate should create the correct AggregateToCollectionOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null, null])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')
        def pipeline = [BasicDBObject.parse('{$match: {}}'), BasicDBObject.parse('{$out: "myColl"}')]
        def bsonPipeline = [BsonDocument.parse('{$match: {}}'), BsonDocument.parse('{$out: "myColl"}')]

        when:
        collection.aggregate(pipeline, AggregationOptions.builder().build())

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getReadConcern(), collection.getWriteConcern()))

        when: // Inherits from DB
        collection.aggregate(pipeline, AggregationOptions.builder().build())

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getReadConcern(), collection.getWriteConcern()))

        when:
        collection.aggregate(pipeline, AggregationOptions.builder().collation(collation).build())

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getReadConcern(), collection.getWriteConcern()).collation(collation))
    }

    def 'explainAggregate should create the correct AggregateOperation'() {
        given:
        def result = BsonDocument.parse('{ok: 1}')
        def executor = new TestOperationExecutor([result, result, result])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')
        def options = AggregationOptions.builder().collation(collation).build()
        def pipeline = [BasicDBObject.parse('{$match: {}}')]
        def bsonPipeline = [BsonDocument.parse('{$match: {}}')]

        when:
        collection.explainAggregate(pipeline, options)

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true).collation(collation)
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER, new BsonDocumentCodec()))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.explainAggregate(pipeline, options)

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true).collation(collation)
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER, new BsonDocumentCodec()))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.explainAggregate(pipeline, options)

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(),
                bsonPipeline, collection.getDefaultDBObjectCodec()).retryReads(true).collation(collation)
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER, new BsonDocumentCodec()))
    }

    def 'update should create the correct UpdateOperation'() {
        given:
        def result = Stub(WriteConcernResult)
        def executor = new TestOperationExecutor([result, result, result])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')
        def query = '{a: 1}'
        def update = '{$set: {a: 2}}'

        when:
        def updateRequest = new UpdateRequest(BsonDocument.parse(query), BsonDocument.parse(update),
                com.mongodb.internal.bulk.WriteRequest.Type.UPDATE).multi(false)
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update))

        then:
        expect executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForUpdate(collection.getNamespace(),
                true, WriteConcern.ACKNOWLEDGED, retryWrites, asList(updateRequest)))

        when: // Inherits from DB
        db.setWriteConcern(WriteConcern.W3)
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update))


        then:
        expect executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForUpdate(collection.getNamespace(),
                true, WriteConcern.W3, retryWrites, asList(updateRequest)))

        when:
        collection.setWriteConcern(WriteConcern.W1)
        updateRequest.collation(collation)
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update),
                new DBCollectionUpdateOptions().collation(collation).arrayFilters(dbObjectArrayFilters))

        then:
        expect executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForUpdate(collection.getNamespace(),
                true, WriteConcern.W1, retryWrites, asList(updateRequest.arrayFilters(bsonDocumentWrapperArrayFilters))))

        where:
        dbObjectArrayFilters <<            [null, [], [new BasicDBObject('i.b', 1)]]
        bsonDocumentWrapperArrayFilters << [null, [], [new BsonDocumentWrapper<BasicDBObject>(new BasicDBObject('i.b', 1),
                DEFAULT_DBOBJECT_CODEC_FACTORY)]]
    }

    def 'remove should create the correct DeleteOperation'() {
        given:
        def result = Stub(WriteConcernResult)
        def executor = new TestOperationExecutor([result, result, result])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def retryWrites = db.getMongoClient().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')
        def query = '{a: 1}'

        when:
        def deleteRequest = new DeleteRequest(BsonDocument.parse(query))
        collection.remove(BasicDBObject.parse(query))

        then:
        expect executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForDelete(collection.getNamespace(),
                false, WriteConcern.ACKNOWLEDGED, retryWrites, asList(deleteRequest)))

        when: // Inherits from DB
        db.setWriteConcern(WriteConcern.W3)
        collection.remove(BasicDBObject.parse(query))

        then:
        expect executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForDelete(collection.getNamespace(),
                false, WriteConcern.W3, retryWrites, asList(deleteRequest)))

        when:
        collection.setWriteConcern(WriteConcern.W1)
        deleteRequest.collation(collation)
        collection.remove(BasicDBObject.parse(query), new DBCollectionRemoveOptions().collation(collation))

        then:
        expect executor.getWriteOperation(), isTheSameAs(createBulkWriteOperationForDelete(collection.getNamespace(),
                false, WriteConcern.W1, retryWrites, asList(deleteRequest)))
    }

    def 'should create the correct MixedBulkWriteOperation'() {
        given:
        def result = Stub(com.mongodb.bulk.BulkWriteResult)
        def executor = new TestOperationExecutor([result, result, result])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')
        def query = '{a: 1}'
        def update = '{$set: {level: 1}}'
        def insertedDocument = new BasicDBObject('_id', 1)
        def insertRequest = new InsertRequest(new BsonDocumentWrapper(insertedDocument, collection.getDefaultDBObjectCodec()))
        def updateRequest = new UpdateRequest(BsonDocument.parse(query), BsonDocument.parse(update),
                com.mongodb.internal.bulk.WriteRequest.Type.UPDATE).multi(false).collation(collation)
                .arrayFilters(bsonDocumentWrapperArrayFilters)
        def deleteRequest = new DeleteRequest(BsonDocument.parse(query)).multi(false).collation(frenchCollation)
        def writeRequests = asList(insertRequest, updateRequest, deleteRequest)

        when:
        def bulk = {
            def bulkOp = ordered ? collection.initializeOrderedBulkOperation() : collection.initializeUnorderedBulkOperation()
            bulkOp.insert(insertedDocument)
            bulkOp.find(BasicDBObject.parse(query)).collation(collation).arrayFilters(dbObjectArrayFilters)
                    .updateOne(BasicDBObject.parse(update))
            bulkOp.find(BasicDBObject.parse(query)).collation(frenchCollation).removeOne()
            bulkOp
        }
        bulk().execute()

        then:
        expect executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(),
                writeRequests, ordered,
                WriteConcern.ACKNOWLEDGED, false))

        when: // Inherits from DB
        db.setWriteConcern(WriteConcern.W3)
        bulk().execute()

        then:
        expect executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(),
                writeRequests, ordered, WriteConcern.W3, false))

        when:
        collection.setWriteConcern(WriteConcern.W1)
        bulk().execute()

        then:
        expect executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(),
                writeRequests, ordered, WriteConcern.W1, false))

        where:
        ordered << [true, false, true]
        dbObjectArrayFilters <<            [null, [], [new BasicDBObject('i.b', 1)]]
        bsonDocumentWrapperArrayFilters << [null, [], [new BsonDocumentWrapper<BasicDBObject>(new BasicDBObject('i.b', 1),
                DEFAULT_DBOBJECT_CODEC_FACTORY)]]
    }

    def collation = Collation.builder()
            .locale('en')
            .caseLevel(true)
            .collationCaseFirst(CollationCaseFirst.OFF)
            .collationStrength(CollationStrength.IDENTICAL)
            .numericOrdering(true)
            .collationAlternate(CollationAlternate.SHIFTED)
            .collationMaxVariable(CollationMaxVariable.SPACE)
            .backwards(true)
            .build()

    def frenchCollation = Collation.builder().locale('fr').build()
}
