package com.mongodb.async.client

import com.mongodb.CursorType
import com.mongodb.MongoNamespace
import com.mongodb.client.model.FindOptions
import com.mongodb.client.options.OperationOptions
import com.mongodb.operation.AsyncBatchCursor
import com.mongodb.operation.FindOperation
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.Document
import org.bson.codecs.BsonValueCodecProvider
import org.bson.codecs.DocumentCodecProvider
import org.bson.codecs.ValueCodecProvider
import org.bson.codecs.configuration.RootCodecRegistry
import spock.lang.Specification

import static com.mongodb.ReadPreference.secondary
import static java.util.concurrent.TimeUnit.MILLISECONDS

class FindFluentSpecification extends Specification {

    def codecs = [new ValueCodecProvider(),
                  new DocumentCodecProvider(),
                  new BsonValueCodecProvider()]
    def options = OperationOptions.builder()
                                  .codecRegistry(new RootCodecRegistry(codecs))
                                  .readPreference(secondary())
                                  .build()

    def 'should build the expected findOperation'() {
        given:
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor, cursor]);
        def findOptions = new FindOptions().sort(new Document('sort', 1))
                                           .modifiers(new Document('modifier', 1))
                                           .projection(new Document('projection', 1))
                                           .maxTime(1000, MILLISECONDS)
                                           .batchSize(100)
                                           .limit(100)
                                           .skip(10)
                                           .cursorType(CursorType.NonTailable)
                                           .oplogReplay(false)
                                           .noCursorTimeout(false)
                                           .partial(false)
        def fluentFind = new FindFluentImpl<Document>(new MongoNamespace('db', 'coll'), options, executor, new Document('filter', 1),
                                                      findOptions, Document)

        when: 'default input should be as expected'
        fluentFind.into([]) { result, t -> }

        def operation = executor.getReadOperation() as FindOperation<Document>
        def readPreference = executor.getReadPreference()

        then:
        operation.filter == new BsonDocument('filter', new BsonInt32(1))
        operation.sort == new BsonDocument('sort', new BsonInt32(1))
        operation.modifiers == new BsonDocument('modifier', new BsonInt32(1))
        operation.projection == new BsonDocument('projection', new BsonInt32(1))
        operation.getMaxTime(MILLISECONDS) == 1000
        operation.batchSize == 100
        operation.limit == 100
        operation.skip == 10
        operation.cursorType == CursorType.NonTailable
        !operation.isTailableCursor()
        !operation.isAwaitData()
        !operation.isOplogReplay()
        !operation.isNoCursorTimeout()
        !operation.isPartial()
        operation.isSlaveOk()
        readPreference == secondary()

        when: 'overriding initial options'
        fluentFind.filter(new Document('filter', 2))
                  .sort(new Document('sort', 2))
                  .modifiers(new Document('modifier', 2))
                  .projection(new Document('projection', 2))
                  .maxTime(999, MILLISECONDS)
                  .batchSize(99)
                  .limit(99)
                  .skip(9)
                  .cursorType(CursorType.Tailable)
                  .oplogReplay(true)
                  .noCursorTimeout(true)
                  .partial(true)
                  .into([]) { result, t -> }

        operation = executor.getReadOperation() as FindOperation<Document>

        then: 'should use the overrides'
        operation.filter == new BsonDocument('filter', new BsonInt32(2))
        operation.sort == new BsonDocument('sort', new BsonInt32(2))
        operation.modifiers == new BsonDocument('modifier', new BsonInt32(2))
        operation.projection == new BsonDocument('projection', new BsonInt32(2))
        operation.getMaxTime(MILLISECONDS) == 999
        operation.batchSize == 99
        operation.limit == 99
        operation.skip == 9
        operation.cursorType == CursorType.Tailable
        operation.isOplogReplay()
        operation.isNoCursorTimeout()
        operation.isPartial()
        operation.isSlaveOk()
    }

    def 'should handle mixed types'() {
        given:
        def cursor = Stub(AsyncBatchCursor) {
            next(_) >> {
                it[0].onResult(null, null)
            }
        }
        def executor = new TestOperationExecutor([cursor]);
        def findOptions = new FindOptions()
        def fluentFind = new FindFluentImpl<Document>(new MongoNamespace('db', 'coll'), options, executor, new Document('filter', 1),
                                                      findOptions, Document)

        when:
        fluentFind.filter(new Document('filter', 1))
                  .sort(new BsonDocument('sort', new BsonInt32(1)))
                  .modifiers(new Document('modifier', 1))
                  .into([]) { result, t -> }

        def operation = executor.getReadOperation() as FindOperation<Document>

        then:
        operation.filter == new BsonDocument('filter', new BsonInt32(1))
        operation.sort == new BsonDocument('sort', new BsonInt32(1))
        operation.modifiers == new BsonDocument('modifier', new BsonInt32(1))
    }

}