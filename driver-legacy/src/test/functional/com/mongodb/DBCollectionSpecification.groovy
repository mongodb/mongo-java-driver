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

import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.IndexRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
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
import com.mongodb.operation.AggregateOperation
import com.mongodb.operation.AggregateToCollectionOperation
import com.mongodb.operation.BatchCursor
import com.mongodb.operation.CommandReadOperation
import com.mongodb.operation.CountOperation
import com.mongodb.operation.CreateIndexesOperation
import com.mongodb.operation.DeleteOperation
import com.mongodb.operation.DistinctOperation
import com.mongodb.operation.FindAndDeleteOperation
import com.mongodb.operation.FindAndReplaceOperation
import com.mongodb.operation.FindAndUpdateOperation
import com.mongodb.operation.FindOperation
import com.mongodb.operation.GroupOperation
import com.mongodb.operation.MapReduceBatchCursor
import com.mongodb.operation.MapReduceStatistics
import com.mongodb.operation.MapReduceToCollectionOperation
import com.mongodb.operation.MapReduceWithInlineResultsOperation
import com.mongodb.operation.MixedBulkWriteOperation
import com.mongodb.operation.ParallelCollectionScanOperation
import com.mongodb.operation.UpdateOperation
import org.bson.BsonDocument
import org.bson.BsonDocumentWrapper
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import org.bson.BsonString
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.BsonValueCodec
import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.mongodb.CustomMatchers.isTheSameAs
import static com.mongodb.Fixture.getMongoClient
import static java.util.Arrays.asList
import static spock.util.matcher.HamcrestSupport.expect

class DBCollectionSpecification extends Specification {

    private static final DEFAULT_DBOBJECT_CODEC_FACTORY = new DBObjectCodec(MongoClient.getDefaultCodecRegistry(),
            DBObjectCodec.getDefaultBsonTypeClassMap(),
            new DBCollectionObjectFactory());

    def 'should throw IllegalArgumentException if name is invalid'() {
        when:
        new DB(getMongoClient(), 'myDatabase', new TestOperationExecutor([])).getCollection('')

        then:
        thrown(IllegalArgumentException)
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
        def executor = new TestOperationExecutor([null, null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def keys = new BasicDBObject('a', 1);

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
                                                      'max'          : new Double(180.0), 'bucketSize': new Double(200.0), 'dropDups': true,
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
                .bucketSize(200.0)
                .dropDups(true)
                .storageEngine(BsonDocument.parse(storageEngine))
                .partialFilterExpression(BsonDocument.parse(partialFilterExpression))
                .collation(collation))
    }

    def 'should support boolean index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('sparse', value);

        when:
        collection.createIndex(new BasicDBObject('y', 1), options);

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
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('expireAfterSeconds', integerValue);

        when:
        collection.createIndex(new BasicDBObject('y', 1), options);

        then:
        def operation = executor.getWriteOperation() as CreateIndexesOperation
        operation.requests[0].getExpireAfter(TimeUnit.SECONDS) == integerValue

        where:
        integerValue << [4, 4L, (double) 4.0]
    }

    def 'should support double index options that are numbers'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('max', doubleValue);

        when:
        collection.createIndex(new BasicDBObject('y', '2d'), options);

        then:
        def operation = executor.getWriteOperation() as CreateIndexesOperation
        operation.requests[0].max == doubleValue

        where:
        doubleValue << [4, 4L, (double) 4.0]
    }

    def 'should throw IllegalArgumentException for unsupported option value type'() {
        given:
        def executor = new TestOperationExecutor([null, null]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        def options = new BasicDBObject('sparse', 'true');


        when:
        collection.createIndex(new BasicDBObject('y', '1'), options);

        then:
        thrown(IllegalArgumentException)
    }

    def 'getStats should execute the expected command with the collection default read preference'() {
        given:
        def executor = new TestOperationExecutor([new BsonDocument('ok', new BsonInt32(1))]);
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadPreference(ReadPreference.secondary())

        when:
        collection.getStats()

        then:
        expect executor.getReadOperation(), isTheSameAs(new CommandReadOperation('myDatabase',
                                                                                 new BsonDocument('collStats', new BsonString('test')),
                                                                new BsonDocumentCodec()))
        executor.getReadPreference() == collection.getReadPreference()
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
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .filter(new BsonDocument())
                .modifiers(new BsonDocument()))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.find().iterator().hasNext()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .filter(new BsonDocument())
                .modifiers(new BsonDocument())
                .readConcern(ReadConcern.MAJORITY))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.find(new BasicDBObject(), new DBCollectionFindOptions().collation(collation)).iterator().hasNext()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .filter(new BsonDocument())
                .modifiers(new BsonDocument())
                .collation(collation)
                .readConcern(ReadConcern.LOCAL))
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
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .filter(new BsonDocument())
                .modifiers(new BsonDocument())
                .limit(-1))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.findOne()

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .filter(new BsonDocument())
                .modifiers(new BsonDocument())
                .limit(-1)
                .readConcern(ReadConcern.MAJORITY))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.findOne(new BasicDBObject(), new DBCollectionFindOptions().collation(collation))

        then:
        expect executor.getReadOperation(), isTheSameAs(new FindOperation(collection.getNamespace(), collection.getObjectCodec())
                .filter(new BsonDocument())
                .modifiers(new BsonDocument())
                .limit(-1)
                .readConcern(ReadConcern.LOCAL)
                .collation(collation))
    }

    def 'findAndRemove should create the correct FindAndDeleteOperation'() {
        given:
        def query = new BasicDBObject()
        def cannedResult = BasicDBObject.parse('{value: {}}')
        def executor = new TestOperationExecutor([cannedResult, cannedResult, cannedResult])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def retryWrites = db.getMongo().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')

        when:
        collection.findAndRemove(query)

        then:
        expect executor.getWriteOperation(), isTheSameAs(new FindAndDeleteOperation<DBObject>(collection.getNamespace(),
                WriteConcern.ACKNOWLEDGED, retryWrites, collection.getObjectCodec()).filter(new BsonDocument()))
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
        def retryWrites = db.getMongo().getMongoClientOptions().getRetryWrites()
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
                .collation(collation).arrayFilters(bsonDocumentWrapperArrayFilters))

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
        def retryWrites = db.getMongo().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')

        when:
        collection.findAndModify(query, replace)

        then:
        expect executor.getWriteOperation(), isTheSameAs(new FindAndReplaceOperation<DBObject>(collection.getNamespace(),
                WriteConcern.ACKNOWLEDGED, retryWrites, collection.getObjectCodec(), bsonReplace)
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
        def executor = new TestOperationExecutor([42L, 42L, 42L]);
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        collection.count()

        then:
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace()).filter(new BsonDocument()))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.count()

        then:
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument())
                .readConcern(ReadConcern.MAJORITY))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.count(new BasicDBObject(), new DBCollectionCountOptions().collation(collation))

        then:
        expect executor.getReadOperation(), isTheSameAs(new CountOperation(collection.getNamespace())
                .filter(new BsonDocument())
                .readConcern(ReadConcern.LOCAL)
                .collation(collation))
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
        def executor = new TestOperationExecutor([cursor, cursor, cursor]);
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        def distinctFieldValues = collection.distinct('field1')

        then:
        distinctFieldValues == [1, 2]
        expect executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), 'field1', new BsonValueCodec())
                                                                .filter(new BsonDocument()))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.distinct('field1')

        then:
        expect executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), 'field1', new BsonValueCodec())
                .filter(new BsonDocument())
                .readConcern(ReadConcern.MAJORITY))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.distinct('field1', new DBCollectionDistinctOptions().collation(collation))

        then:
        expect executor.getReadOperation(), isTheSameAs(new DistinctOperation(collection.getNamespace(), 'field1', new BsonValueCodec())
                .readConcern(ReadConcern.LOCAL)
                .collation(collation))
    }

    def 'group should create the correct GroupOperation'() {
        given:
        def cursor = Stub(BatchCursor) {
            next() >> []
            hasNext() >> false
        }
        def executor = new TestOperationExecutor([cursor, cursor, cursor])
        def key = BasicDBObject.parse('{name: 1}')
        def reduce = 'function ( curr, result ) { }'
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')

        when:
        collection.group(key, new BasicDBObject(), new BasicDBObject(), reduce)

        then:
        expect executor.getReadOperation(), isTheSameAs(new GroupOperation<DBObject>(collection.getNamespace(), new BsonJavaScript(reduce),
                new BsonDocument(), collection.getDefaultDBObjectCodec()).key(BsonDocument.parse('{name: 1}'))
                .filter(new BsonDocument()))

        when: // Can set collation
        def groupCommand = new GroupCommand(collection, key, new BasicDBObject(), new BasicDBObject(), reduce, null, collation)
        collection.group(groupCommand)

        then:
        expect executor.getReadOperation(), isTheSameAs(new GroupOperation<DBObject>(collection.getNamespace(), new BsonJavaScript(reduce),
                new BsonDocument(), collection.getDefaultDBObjectCodec()).key(BsonDocument.parse('{name: 1}')).collation(collation)
                .filter(new BsonDocument())
        )
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
                new MapReduceWithInlineResultsOperation(collection.getNamespace(), new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                        collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument())
        )

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.LOCAL)
        collection.mapReduce('map', 'reduce', null, MapReduceCommand.OutputType.INLINE, new BasicDBObject())

        then:
        expect executor.getReadOperation(), isTheSameAs(
                new MapReduceWithInlineResultsOperation(collection.getNamespace(), new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                        collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument())
                        .readConcern(ReadConcern.LOCAL)
        )

        when:
        collection.setReadConcern(ReadConcern.MAJORITY)
        def mapReduceCommand = new MapReduceCommand(collection, 'map', 'reduce', null, MapReduceCommand.OutputType.INLINE,
                new BasicDBObject())
        mapReduceCommand.setCollation(collation)
        collection.mapReduce(mapReduceCommand)

        then:
        expect executor.getReadOperation(), isTheSameAs(
                new MapReduceWithInlineResultsOperation(collection.getNamespace(), new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                        collection.getDefaultDBObjectCodec())
                        .verbose(true)
                        .filter(new BsonDocument())
                        .readConcern(ReadConcern.MAJORITY)
                        .collation(collation)
        )
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
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                        'myColl', collection.getWriteConcern())
                        .verbose(true)
                        .filter(new BsonDocument())
        )

        when: // Inherits from DB
        collection.mapReduce('map', 'reduce', 'myColl', MapReduceCommand.OutputType.REPLACE, new BasicDBObject())

        then:
        expect executor.getWriteOperation(), isTheSameAs(
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                        'myColl', collection.getWriteConcern())
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
                new MapReduceToCollectionOperation(collection.getNamespace(), new BsonJavaScript('map'), new BsonJavaScript('reduce'),
                        'myColl', collection.getWriteConcern())
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
        collection.aggregate(pipeline)

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(), bsonPipeline,
                collection.getDefaultDBObjectCodec()).useCursor(true))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.aggregate(pipeline)

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(), bsonPipeline,
                collection.getDefaultDBObjectCodec()).useCursor(true).readConcern(ReadConcern.MAJORITY))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.aggregate(pipeline, AggregationOptions.builder().collation(collation).build())

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(), bsonPipeline,
                collection.getDefaultDBObjectCodec()).useCursor(true).readConcern(ReadConcern.LOCAL).collation(collation))
    }

    def 'aggregate should create the correct AggregateToCollectionOperation'() {
        given:
        def executor = new TestOperationExecutor([null, null, null])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def collection = db.getCollection('test')
        def pipeline = [BasicDBObject.parse('{$match: {}}'), BasicDBObject.parse('{$out: "myColl"}')]
        def bsonPipeline = [BsonDocument.parse('{$match: {}}'), BsonDocument.parse('{$out: "myColl"}')]

        when:
        collection.aggregate(pipeline)

        then:
        expect executor.getWriteOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getWriteConcern()))

        when: // Inherits from DB
        collection.aggregate(pipeline)

        then:
        expect executor.getWriteOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getWriteConcern()))

        when:
        collection.aggregate(pipeline, AggregationOptions.builder().collation(collation).build())

        then:
        expect executor.getWriteOperation(), isTheSameAs(new AggregateToCollectionOperation(collection.getNamespace(),
                bsonPipeline, collection.getWriteConcern()).collation(collation))
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
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(), bsonPipeline,
                collection.getDefaultDBObjectCodec()).collation(collation).asExplainableOperation(ExplainVerbosity.QUERY_PLANNER))

        when: // Inherits from DB
        db.setReadConcern(ReadConcern.MAJORITY)
        collection.explainAggregate(pipeline, options)

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(), bsonPipeline,
                collection.getDefaultDBObjectCodec()).readConcern(ReadConcern.MAJORITY).collation(collation)
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER))

        when:
        collection.setReadConcern(ReadConcern.LOCAL)
        collection.explainAggregate(pipeline, options)

        then:
        expect executor.getReadOperation(), isTheSameAs(new AggregateOperation(collection.getNamespace(), bsonPipeline,
                collection.getDefaultDBObjectCodec()).useCursor(false).readConcern(ReadConcern.LOCAL).collation(collation)
                .asExplainableOperation(ExplainVerbosity.QUERY_PLANNER))
    }

    def 'parallel should create the correct ParallelCollectionScanOperation'() {
        given:
        def executor = new TestOperationExecutor([[Stub(BatchCursor) {
            hasNext() >> {
                false
            }
        }]])
        def collection = new DB(getMongoClient(), 'myDatabase', executor).getCollection('test')
        collection.setReadConcern(ReadConcern.MAJORITY)

        when:
        def cursors = collection.parallelScan(ParallelScanOptions.builder().build())

        then:
        cursors.size() == 1
        expect executor.getReadOperation(), isTheSameAs(new ParallelCollectionScanOperation(collection.getNamespace(), 1,
                                                                                            collection.getObjectCodec())
                                                                .readConcern(ReadConcern.MAJORITY)
        )
    }

    def 'update should create the correct UpdateOperation'() {
        given:
        def result = Stub(WriteConcernResult)
        def executor = new TestOperationExecutor([result, result, result])
        def db = new DB(getMongoClient(), 'myDatabase', executor)
        def retryWrites = db.getMongo().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')
        def query = '{a: 1}'
        def update = '{$set: {a: 2}}'

        when:
        def updateRequest = new UpdateRequest(BsonDocument.parse(query), BsonDocument.parse(update),
                com.mongodb.bulk.WriteRequest.Type.UPDATE).multi(false)
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update))

        then:
        expect executor.getWriteOperation(), isTheSameAs(new UpdateOperation(collection.getNamespace(), true,
                WriteConcern.ACKNOWLEDGED, retryWrites, asList(updateRequest)))

        when: // Inherits from DB
        db.setWriteConcern(WriteConcern.W3)
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update))


        then:
        expect executor.getWriteOperation(), isTheSameAs(new UpdateOperation(collection.getNamespace(), true,
                WriteConcern.W3, retryWrites, asList(updateRequest)))

        when:
        collection.setWriteConcern(WriteConcern.W1)
        updateRequest.collation(collation)
        collection.update(BasicDBObject.parse(query), BasicDBObject.parse(update),
                new DBCollectionUpdateOptions().collation(collation).arrayFilters(dbObjectArrayFilters))

        then:
        expect executor.getWriteOperation(), isTheSameAs(new UpdateOperation(collection.getNamespace(), true,
                WriteConcern.W1, retryWrites, asList(updateRequest.arrayFilters(bsonDocumentWrapperArrayFilters))))

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
        def retryWrites = db.getMongo().getMongoClientOptions().getRetryWrites()
        def collection = db.getCollection('test')
        def query = '{a: 1}'

        when:
        def deleteRequest = new DeleteRequest(BsonDocument.parse(query))
        collection.remove(BasicDBObject.parse(query))

        then:
        expect executor.getWriteOperation(), isTheSameAs(new DeleteOperation(collection.getNamespace(), false,
                WriteConcern.ACKNOWLEDGED, retryWrites, asList(deleteRequest)))

        when: // Inherits from DB
        db.setWriteConcern(WriteConcern.W3)
        collection.remove(BasicDBObject.parse(query))

        then:
        expect executor.getWriteOperation(), isTheSameAs(new DeleteOperation(collection.getNamespace(), false,
                WriteConcern.W3, retryWrites, asList(deleteRequest)))

        when:
        collection.setWriteConcern(WriteConcern.W1)
        deleteRequest.collation(collation)
        collection.remove(BasicDBObject.parse(query), new DBCollectionRemoveOptions().collation(collation))

        then:
        expect executor.getWriteOperation(), isTheSameAs(new DeleteOperation(collection.getNamespace(), false,
                WriteConcern.W1, retryWrites, asList(deleteRequest)))
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
                com.mongodb.bulk.WriteRequest.Type.UPDATE).multi(false).collation(collation)
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
        expect executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(), writeRequests, ordered,
                WriteConcern.ACKNOWLEDGED, false))

        when: // Inherits from DB
        db.setWriteConcern(WriteConcern.W3)
        bulk().execute()

        then:
        expect executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(), writeRequests, ordered,
                WriteConcern.W3, false))

        when:
        collection.setWriteConcern(WriteConcern.W1)
        bulk().execute()

        then:
        expect executor.getWriteOperation(), isTheSameAs(new MixedBulkWriteOperation(collection.getNamespace(), writeRequests, ordered,
                WriteConcern.W1, false))

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
