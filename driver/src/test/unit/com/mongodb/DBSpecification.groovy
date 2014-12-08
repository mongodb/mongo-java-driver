package com.mongodb

import com.mongodb.operation.CreateCollectionOperation
import org.bson.BsonDocument
import spock.lang.Specification

import static com.mongodb.CustomMatchers.isTheSameAs
import static spock.util.matcher.HamcrestSupport.expect

class DBSpecification extends Specification {

    def 'should execute CreateCollectionOperation'() {
        given:
        def mongo = Stub(Mongo)
        mongo.mongoClientOptions >> MongoClientOptions.builder().build()
        def executor = new TestOperationExecutor([1L, 2L, 3L])
        def db = new DB(mongo, 'test', executor)
        def options = new BasicDBObject()
                .append('size', 100000)
                .append('max', 2000)
                .append('capped', true)
                .append('autoIndexId', true)
                .append('storageEngine', new BasicDBObject('wiredTiger', new BasicDBObject()))

        when:
        db.createCollection('ctest', options)

        then:
        def operation = executor.getWriteOperation() as CreateCollectionOperation
        operation.storageEngineOptions == new BsonDocument('wiredTiger', new BsonDocument())
        expect operation, isTheSameAs(new CreateCollectionOperation('test', 'ctest')
                                              .sizeInBytes(100000)
                                              .maxDocuments(2000)
                                              .capped(true)
                                              .autoIndex(true)
                                              .storageEngineOptions(new BsonDocument('wiredTiger', new BsonDocument())))
    }
}