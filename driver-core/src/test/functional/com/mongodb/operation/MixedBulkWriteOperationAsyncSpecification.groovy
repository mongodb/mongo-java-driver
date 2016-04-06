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
import com.mongodb.ClusterFixture
import com.mongodb.MongoBulkWriteException
import com.mongodb.MongoClientException
import com.mongodb.MongoNamespace
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.BulkWriteUpsert
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.codecs.DocumentCodec
import org.bson.types.ObjectId
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.executeAsync
import static com.mongodb.ClusterFixture.getAsyncSingleConnectionBinding
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.bulk.WriteRequest.Type.DELETE
import static com.mongodb.bulk.WriteRequest.Type.INSERT
import static com.mongodb.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.bulk.WriteRequest.Type.UPDATE
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.gte
import static java.util.Arrays.asList

@Category(Async)
class MixedBulkWriteOperationAsyncSpecification extends OperationFunctionalSpecification {

    def 'when no document with the same id exists, should insert the document'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))], ordered,
                                             ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result.insertedCount == 1
        result.upserts == []
        getCollectionHelper().count() == 1

        where:
        ordered << [true, false]
    }

    def 'when a document with the same id exists, should throw an exception'() {
        given:
        def document = new BsonDocument('_id', new BsonInt32(1))
        getCollectionHelper().insertDocuments(document)
        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest(document)], ordered, ACKNOWLEDGED)

        when:
        executeAsync(op)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteErrors().get(0).code == 11000

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, a remove of one should remove one of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new DeleteRequest(new BsonDocument('x', BsonBoolean.TRUE)).multi(false)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(DELETE, 1, [])
        getCollectionHelper().count() == 1

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, a remove should remove all of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                                              new Document('x', true),
                                              new Document('x', true),
                                              new Document('x', false))

        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new DeleteRequest(new BsonDocument('x', BsonBoolean.TRUE))],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(DELETE, 2, [])
        getCollectionHelper().count() == 1

        where:
        ordered << [true, false]
    }

    def 'when multiple document match the query, update of one should update only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))),
                                                                UPDATE)
                                                      .multi(false)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count(new Document('y', 1)) == 1

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, update multi should update all of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))),
                                                                UPDATE)
                                                      .multi(true)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 2, expectedModifiedCount(2), [])
        getCollectionHelper().count(new Document('y', 1)) == 2

        where:
        ordered << [true, false]
    }

    def 'when no document matches the query, an update of one with upsert should insert a document'() {
        def id = new ObjectId()
        def query = new BsonDocument('_id', new BsonObjectId(id))
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(query, new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))),
                                                                UPDATE)
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonObjectId(id))])
        getCollectionHelper().find().first() == new Document('_id', query.getObjectId('_id').getValue()).append('x', 2)

        where:
        ordered << [true, false]
    }

    def 'when no document matches the query, an update multi with upsert should insert a document'() {
        def id = new ObjectId()
        def query = new BsonDocument('_id', new BsonObjectId(id))
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(query, new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))),
                                                                UPDATE)
                                                      .upsert(true).multi(true)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonObjectId(id))])
        getCollectionHelper().find().first() == new Document('_id', query.getObjectId('_id').getValue()).append('x', 2)

        where:
        ordered << [true, false]
    }

    def 'when documents matches the query, update one with upsert should update only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))),
                                                                UPDATE)
                                                      .multi(false)
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count(new Document('y', 1)) == 1

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, update multi with upsert should update all of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))),
                                                                UPDATE)
                                                      .upsert(true).multi(true)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 2, expectedModifiedCount(2), [])
        getCollectionHelper().count(new Document('y', 1)) == 2

        where:
        ordered << [true, false]
    }


    def 'when updating with an empty document, update should throw IllegalArgumentException'() {
        given:
        def id = new ObjectId()
        def op = new MixedBulkWriteOperation(getNamespace(),
                [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)), new BsonDocument(), UPDATE)],
                true, ACKNOWLEDGED)

        when:
        executeAsync(op)

        then:
        thrown(IllegalArgumentException)

        where:
        ordered << [true, false]
    }

    def 'when updating with an invalid document, update should throw IllegalArgumentException'() {
        given:
        def id = new ObjectId()
        def op = new MixedBulkWriteOperation(getNamespace(),
                [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)), new BsonDocument('a', new BsonInt32(1)), UPDATE)],
                true, ACKNOWLEDGED)

        when:
        executeAsync(op)

        then:
        thrown(IllegalArgumentException)

        where:
        ordered << [true, false]
    }


    def 'when a document contains a key with an illegal character, replacing a document with it should throw IllegalArgumentException'() {
        given:
        def id = new ObjectId()
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1))),
                                                                REPLACE)
                                                      .upsert(true)],
                                             true, ACKNOWLEDGED)

        when:
        executeAsync(op)

        then:
        thrown(IllegalArgumentException)

        where:
        ordered << [true, false]
    }

    def 'when no document matches the query, a replace with upsert should insert a document'() {
        given:
        def id = new ObjectId()
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)),
                                                                new BsonDocument('_id', new BsonObjectId(id)).append('x', new BsonInt32(2)),
                                                                REPLACE)
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonObjectId(id))])
        getCollectionHelper().find().first() == new Document('_id', id).append('x', 2)

        where:
        ordered << [true, false]
    }

    def 'when a custom _id is upserted it should be in the write result'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonInt32(0)),
                                                                new BsonDocument('$set', new BsonDocument('a', new BsonInt32(0))),
                                                                UPDATE)
                                                      .upsert(true),
                                              new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                new BsonDocument('_id', new BsonInt32(1)),
                                                                REPLACE)
                                                      .upsert(true),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                new BsonDocument('_id', new BsonInt32(2)),
                                                                REPLACE)
                                                      .upsert(true)
                                             ],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonInt32(0)),
                                                                                        new BulkWriteUpsert(1, new BsonInt32(1)),
                                                                                        new BulkWriteUpsert(2, new BsonInt32(2))])
        getCollectionHelper().count() == 3

        where:
        ordered << [true, false]
    }

    def 'unacknowledged upserts with custom _id should not error'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonInt32(0)),
                                                                new BsonDocument('$set', new BsonDocument('a', new BsonInt32(0))),
                                                                UPDATE)
                                                      .upsert(true),
                                              new UpdateRequest(new BsonDocument('a', new BsonInt32(1)),
                                                                new BsonDocument('_id', new BsonInt32(1)),
                                                                REPLACE)
                                                      .upsert(true),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                new BsonDocument('_id', new BsonInt32(2)),
                                                                REPLACE)
                                                      .upsert(true)
                                             ],
                                             ordered, UNACKNOWLEDGED)

        when:
        def binding = getAsyncSingleConnectionBinding()
        def result = executeAsync(op, binding)

        then:
        !result.wasAcknowledged()
        acknowledgeWrite(binding)
        getCollectionHelper().count() == 4

        where:
        ordered << [true, false]
    }

    def 'when multiple documents match the query, replace should replace only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true))

        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('y', new BsonInt32(1)).append('x', BsonBoolean.FALSE),
                                                                REPLACE)
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count(new Document('x', false)) == 1

        where:
        ordered << [true, false]
    }

    @Category([Async, Slow])
    def 'when a replacement document is 16MB, the document is still replaced'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                new BsonDocument('_id', new BsonInt32(1))
                                                                        .append('x', new BsonBinary(new byte[1024 * 1024 * 16 - 30])),
                                                                REPLACE)
                                                      .upsert(true)],
                                             true, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count() == 1
    }

    @Category([Async, Slow])
    def 'when two update documents together exceed 16MB, the documents are still updated'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1), new Document('_id', 2))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                new BsonDocument('_id', new BsonInt32(1))
                                                                        .append('x', new BsonBinary(new byte[1024 * 1024 * 16 - 30])),
                                                                REPLACE),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                new BsonDocument('_id', new BsonInt32(2))
                                                                        .append('x', new BsonBinary(new byte[1024 * 1024 * 16 - 30])),
                                                                REPLACE)],
                                             true, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 2, expectedModifiedCount(2), [])
        getCollectionHelper().count() == 2
    }

    def 'should handle multi-length runs of ordered insert, update, replace, and remove'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(), getTestWrites(), ordered, ACKNOWLEDGED)

        when:
        def result = executeAsync(op)

        then:
        result.wasAcknowledged()
        getCollectionHelper().find(new Document('_id', 1)).first() == new Document('_id', 1).append('x', 2)
        getCollectionHelper().find(new Document('_id', 2)).first() == new Document('_id', 2).append('x', 3)
        getCollectionHelper().find(new Document('_id', 3)).isEmpty()
        getCollectionHelper().find(new Document('_id', 4)).isEmpty()
        getCollectionHelper().find(new Document('_id', 5)).first() == new Document('_id', 5).append('x', 4)
        getCollectionHelper().find(new Document('_id', 6)).first() == new Document('_id', 6).append('x', 5)
        getCollectionHelper().find(new Document('_id', 7)).first() == new Document('_id', 7)
        getCollectionHelper().find(new Document('_id', 8)).first() == new Document('_id', 8)

        where:
        ordered << [true, false]
    }

    def 'should handle multi-length runs of unacknowledged insert, update, replace, and remove'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(), getTestWrites(), ordered, UNACKNOWLEDGED)

        when:
        def binding = getAsyncSingleConnectionBinding()
        def result = executeAsync(op, binding)

        then:
        !result.wasAcknowledged()
        acknowledgeWrite(binding)
        getCollectionHelper().find(new Document('_id', 1)).first() == new Document('_id', 1).append('x', 2)
        getCollectionHelper().find(new Document('_id', 2)).first() == new Document('_id', 2).append('x', 3)
        getCollectionHelper().find(new Document('_id', 3)).isEmpty()
        getCollectionHelper().find(new Document('_id', 4)).isEmpty()
        getCollectionHelper().find(new Document('_id', 5)).first() == new Document('_id', 5).append('x', 4)
        getCollectionHelper().find(new Document('_id', 6)).first() == new Document('_id', 6).append('x', 5)
        getCollectionHelper().find(new Document('_id', 7)).first() == new Document('_id', 7)
        getCollectionHelper().find(new Document('_id', 8)).first() == new Document('_id', 8)


        where:
        ordered << [true, false]
    }

    def 'should split the number of writes is larger than the match write batch size'() {
        given:
        def writes = []
        (0..2000).each {
            writes.add(new InsertRequest(new BsonDocument()))
        }

        when:
        executeAsync(new MixedBulkWriteOperation(getNamespace(), writes, ordered, ACKNOWLEDGED))

        then:
        getCollectionHelper().count() == 2001

        where:
        ordered << [true, false]
    }

    def 'should be able to merge upserts across batches'() {
        given:
        def writeOperations = [];
        (0..1002).each {
            def upsert = new UpdateRequest(new BsonDocument('key', new BsonInt32(it)),
                    new BsonDocument('$set', new BsonDocument('key', new BsonInt32(it))),
                    UPDATE).upsert(true)
            writeOperations.add(upsert);
            writeOperations.add(new DeleteRequest(new BsonDocument('key', new BsonInt32(it))));
        }

        when:
        def result = executeAsync(new MixedBulkWriteOperation(getNamespace(), writeOperations, ordered, ACKNOWLEDGED))

        then:
        result.deletedCount == result.upserts.size()
        getCollectionHelper().count() == 0

        where:
        ordered << [true, false]
    }

    def 'error details should have correct index on ordered write failure'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3))),
                                                                UPDATE),
                                              new InsertRequest(new BsonDocument('_id', new BsonInt32(1))) // this should fail with index 2
                                             ], true, ACKNOWLEDGED)
        when:
        executeAsync(op)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 2
        ex.writeErrors[0].code == 11000
    }

    def 'error details should have correct index on unordered write failure'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3))),
                                                                UPDATE),
                                              new InsertRequest(new BsonDocument('_id', new BsonInt32(3))) // this should fail with index 2
                                             ], false, ACKNOWLEDGED)
        when:
        executeAsync(op)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 2
        ex.writeErrors[0].index == 0
        ex.writeErrors[0].code == 11000
        ex.writeErrors[1].index == 2
        ex.writeErrors[1].code == 11000
    }

    // using w = 5 to force a timeout
    @IgnoreIf({ !ClusterFixture.isDiscoverableReplicaSet() })
    def 'should throw bulk write exception with a write concern error when wtimeout is exceeded'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                             false, new WriteConcern(5, 1)
        )
        when:
        executeAsync(op)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteConcernError() != null
    }

    @IgnoreIf({ !ClusterFixture.isDiscoverableReplicaSet() })
    def 'when there is a duplicate key error and a write concern error, both should be reported'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(7))),
                                              new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))   // duplicate key
                                             ], ordered, new WriteConcern(4, 1))

        when:
        executeAsync(op)  // This is assuming that it won't be able to replicate to 4 servers in 1 ms

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 1
        ex.writeErrors[0].code == 11000
        ex.writeConcernError != null

        where:
        ordered << [false]
    }

    def 'should throw IllegalArgumentException when passed an empty bulk operation'() {

        when:
        new MixedBulkWriteOperation(getNamespace(), [], ordered, UNACKNOWLEDGED)

        then:
        thrown(IllegalArgumentException)

        where:
        ordered << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should honour the bypass validation flag for inserts'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collection')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create(namespace.getCollectionName(), new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        def op = new MixedBulkWriteOperation(namespace, [new InsertRequest(BsonDocument.parse('{ level: 9 }'))], ordered,
                ACKNOWLEDGED)

        when:
        executeAsync(op)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteErrors().get(0).code == 121

        when:
        def result = executeAsync(op.bypassDocumentValidation(true))

        then:
        notThrown(MongoBulkWriteException)
        result == BulkWriteResult.acknowledged(INSERT, 1, 0, [])

        cleanup:
        collectionHelper?.drop()

        where:
        ordered << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 1, 8)) })
    def 'should honour the bypass validation flag for updates'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collection')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create(namespace.getCollectionName(), new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))

        collectionHelper.insertDocuments(BsonDocument.parse('{ x: true, level: 10}'))
        def op = new MixedBulkWriteOperation(namespace,
                [new UpdateRequest(BsonDocument.parse ('{x: true}'), BsonDocument.parse ('{$inc: {level: -1}}'),  UPDATE).multi(false)],
                ordered, ACKNOWLEDGED)

        when:
        executeAsync(op)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteErrors().get(0).code == 121

        when:
        def result = executeAsync(op.bypassDocumentValidation(true))

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [])
        collectionHelper.count(eq('level', 9)) == 1

        where:
        ordered << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(asList(3, 2, 0)) })
    def 'should throw if bypassDocumentValidation is set and write is unacknowledged'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest(BsonDocument.parse('{ level: 9 }'))], true, UNACKNOWLEDGED)
                .bypassDocumentValidation(bypassDocumentValidation)

        when:
        executeAsync(op)

        then:
        thrown(MongoClientException)

        where:
        bypassDocumentValidation << [true, false]
    }

    private static List<WriteRequest> getTestWrites() {
        [new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                           new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))),
                           UPDATE),
         new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                           new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3))),
                           UPDATE),
         new DeleteRequest(new BsonDocument('_id', new BsonInt32(3))),
         new DeleteRequest(new BsonDocument('_id', new BsonInt32(4))),
         new UpdateRequest(new BsonDocument('_id', new BsonInt32(5)),
                           new BsonDocument('_id', new BsonInt32(5)).append('x', new BsonInt32(4)),
                           REPLACE),
         new UpdateRequest(new BsonDocument('_id', new BsonInt32(6)),
                           new BsonDocument('_id', new BsonInt32(6)).append('x', new BsonInt32(5)),
                           REPLACE),
         new InsertRequest(new BsonDocument('_id', new BsonInt32(7))),
         new InsertRequest(new BsonDocument('_id', new BsonInt32(8)))
        ]
    }

    private static Document[] getTestInserts() {


        [new Document('_id', 1),
         new Document('_id', 2),
         new Document('_id', 3),
         new Document('_id', 4),
         new Document('_id', 5),
         new Document('_id', 6)]
    }

    private static Integer expectedModifiedCount(final int expectedCountForServersThatSupportIt) {
        (serverVersionAtLeast([2, 6, 0])) ? expectedCountForServersThatSupportIt : null
    }
}
