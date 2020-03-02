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

package com.mongodb.internal.operation

import category.Slow
import com.mongodb.MongoBulkWriteException
import com.mongodb.MongoClientException
import com.mongodb.MongoNamespace
import com.mongodb.MongoSocketException
import com.mongodb.MongoSocketReadException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.bulk.BulkWriteInsert
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.BulkWriteUpsert
import com.mongodb.client.model.CreateCollectionOptions
import com.mongodb.client.model.ValidationOptions
import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.bulk.UpdateRequest
import com.mongodb.internal.bulk.WriteRequest
import org.bson.BsonBinary
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonInt64
import org.bson.BsonObjectId
import org.bson.BsonString
import org.bson.Document
import org.bson.RawBsonDocument
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import org.bson.types.ObjectId
import org.junit.experimental.categories.Category
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.configureFailPoint
import static com.mongodb.ClusterFixture.disableFailPoint
import static com.mongodb.ClusterFixture.disableOnPrimaryTransactionalWriteFailPoint
import static com.mongodb.ClusterFixture.enableOnPrimaryTransactionalWriteFailPoint
import static com.mongodb.ClusterFixture.getAsyncSingleConnectionBinding
import static com.mongodb.ClusterFixture.getSingleConnectionBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionAtLeast
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.client.model.Filters.eq
import static com.mongodb.client.model.Filters.gte
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.bulk.WriteRequest.Type.DELETE
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE

@SuppressWarnings('ClassSize')
class MixedBulkWriteOperationSpecification extends OperationFunctionalSpecification {

    def 'should throw IllegalArgumentException for empty list of requests'() {
        when:
        new MixedBulkWriteOperation(getNamespace(), [], true, ACKNOWLEDGED, false)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should have the expected passed values'() {
        when:
        def operation = new MixedBulkWriteOperation(getNamespace(), requests, ordered, writeConcern, retryWrites)
                .bypassDocumentValidation(bypassValidation)

        then:
        operation.isOrdered() == ordered
        operation.getNamespace() == getNamespace()
        operation.getWriteRequests() == requests
        operation.getRetryWrites() == retryWrites
        operation.getWriteConcern() == writeConcern
        operation.getBypassDocumentValidation() == bypassValidation

        where:
        ordered | writeConcern   | bypassValidation | retryWrites | requests
        true    | ACKNOWLEDGED   | null             | true        | [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))]
        false   | UNACKNOWLEDGED | true             | false       | [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))]
        false   | UNACKNOWLEDGED | false            | false       | [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))]
    }

    def 'when no document with the same id exists, should insert the document'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result.insertedCount == 1
        result.inserts == [new BulkWriteInsert(0, new BsonInt32(1))]
        result.upserts == []
        getCollectionHelper().count() == 1

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when a document with the same id exists, should throw an exception'() {
        given:
        def document = new BsonDocument('_id', new BsonInt32(1))
        getCollectionHelper().insertDocuments(document)
        def operation = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest(document)], ordered, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteErrors().get(0).code == 11000

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'RawBsonDocument should not generate an _id'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest(RawBsonDocument.parse('{_id: 1}'))],
                ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result.insertedCount == 1
        result.inserts == [new BulkWriteInsert(0, null)]
        result.upserts == []
        getCollectionHelper().count() == 1

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when documents match the query, a remove of one should remove one of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new DeleteRequest(new BsonDocument('x', BsonBoolean.TRUE)).multi(false)],
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(DELETE, 1, 0, [], [])
        getCollectionHelper().count() == 1

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when documents match the query, a remove should remove all of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true),
                                              new Document('x', false))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new DeleteRequest(new BsonDocument('x', BsonBoolean.TRUE))],
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(DELETE, 2, 0, [], [])
        getCollectionHelper().count() == 1

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when multiple document match the query, update of one should update only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))),
                                                                UPDATE).multi(false)],
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [], [])
        getCollectionHelper().count(new Document('y', 1)) == 1

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when documents match the query, update multi should update all of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                        new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))),
                        UPDATE).multi(true)], ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 2, expectedModifiedCount(2), [], [])
        getCollectionHelper().count(new Document('y', 1)) == 2

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when no document matches the query, an update of one with upsert should insert a document'() {
        given:
        def id = new ObjectId()
        def query = new BsonDocument('_id', new BsonObjectId(id))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new UpdateRequest(query, new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))),
                        UPDATE).upsert(true)], ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonObjectId(id))], [])
        getCollectionHelper().find().first() == new Document('_id', query.getObjectId('_id').getValue()).append('x', 2)

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when no document matches the query, an update multi with upsert should insert a document'() {
        def id = new ObjectId()
        def query = new BsonDocument('_id', new BsonObjectId(id))
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(query, new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))),
                                                                UPDATE).upsert(true).multi(true)],
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 0, expectedModifiedCount(0),
                [new BulkWriteUpsert(0, new BsonObjectId(id))], [])
        getCollectionHelper().find().first() == new Document('_id', query.getObjectId('_id').getValue()).append('x', 2)

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when documents matches the query, update one with upsert should update only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))),
                                                                UPDATE).multi(false).upsert(true)],
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [], [])
        getCollectionHelper().count(new Document('y', 1)) == 1

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when documents match the query, update multi with upsert should update all of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))),
                                                                UPDATE).upsert(true).multi(true)],
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 2, expectedModifiedCount(2), [], [])
        getCollectionHelper().count(new Document('y', 1)) == 2

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when updating with an empty document, update should throw IllegalArgumentException'() {
        given:
        def id = new ObjectId()
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)), new BsonDocument(), UPDATE)],
                true, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        thrown(IllegalArgumentException)

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when replacing with an empty document, update should not throw IllegalArgumentException'() {
        given:
        def id = new ObjectId()
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)), new BsonDocument(), REPLACE)],
                true, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        noExceptionThrown()

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when updating with an invalid document, update should throw IllegalArgumentException'() {
        given:
        def id = new ObjectId()
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)), new BsonDocument('a', new BsonInt32(1)), UPDATE)],
                true, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        thrown(IllegalArgumentException)

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when a document contains a key with an illegal character, replacing a document with it should throw IllegalArgumentException'() {
        given:
        def id = new ObjectId()
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1))),
                                                                REPLACE)
                                                      .upsert(true)],
                                             true, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        thrown(IllegalArgumentException)

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when no document matches the query, a replace with upsert should insert a document'() {
        given:
        def id = new ObjectId()
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)),
                                                                new BsonDocument('_id', new BsonObjectId(id))
                                                                        .append('x', new BsonInt32(2)),
                                                                REPLACE)
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonObjectId(id))], [])
        getCollectionHelper().find().first() == new Document('_id', id).append('x', 2)

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when a custom _id is upserted it should be in the write result'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(),
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
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 0, expectedModifiedCount(0),
                [new BulkWriteUpsert(0, new BsonInt32(0)), new BulkWriteUpsert(1, new BsonInt32(1)),
                 new BulkWriteUpsert(2, new BsonInt32(2))], [])
        getCollectionHelper().count() == 3

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'unacknowledged upserts with custom _id should not error'() {
        given:
        def binding = async ? getAsyncSingleConnectionBinding() : getSingleConnectionBinding()
        def operation = new MixedBulkWriteOperation(getNamespace(),
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
                                             ordered, UNACKNOWLEDGED, false)

        when:
        def result = execute(operation, binding)
        acknowledgeWrite(binding)

        then:
        !result.wasAcknowledged()
        getCollectionHelper().count() == 4

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'when multiple documents match the query, replace should replace only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true))

        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('y', new BsonInt32(1)).append('x', BsonBoolean.FALSE),
                                                                REPLACE).upsert(true)],
                                             ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [], [])
        getCollectionHelper().count(new Document('x', false)) == 1

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    @Category(Slow)
    def 'when a replacement document is 16MB, the document is still replaced'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                        new BsonDocument('_id', new BsonInt32(1))
                                .append('x', new BsonBinary(new byte[1024 * 1024 * 16 - 30])),
                        REPLACE).upsert(true)], true, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [], [])
        getCollectionHelper().count() == 1

        where:
        async << [true, false]
    }

    @Category(Slow)
    def 'when two update documents together exceed 16MB, the documents are still updated'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1), new Document('_id', 2))
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                new BsonDocument('_id', new BsonInt32(1))
                                                                        .append('x', new BsonBinary(new byte[1024 * 1024 * 16 - 30])),
                                                                REPLACE),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                new BsonDocument('_id', new BsonInt32(2))
                                                                        .append('x', new BsonBinary(new byte[1024 * 1024 * 16 - 30])),
                                                                REPLACE)],
                                             true, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 2, expectedModifiedCount(2), [], [])
        getCollectionHelper().count() == 2

        where:
        async << [true, false]
    }

    @Category(Slow)
    def 'when documents together are just below the max message size, the documents are still inserted'() {
        given:
        def bsonBinary = new BsonBinary(new byte[16 * 1000 * 1000 - (getCollectionName().length() + 33)])
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [
                        new InsertRequest(new BsonDocument('_id', new BsonObjectId()).append('b', bsonBinary)),
                        new InsertRequest(new BsonDocument('_id', new BsonObjectId()).append('b', bsonBinary)),
                        new InsertRequest(new BsonDocument('_id', new BsonObjectId()).append('b', bsonBinary))
                ],
                true, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, true)

        then:
        result.wasAcknowledged()
        result.insertedCount == 3
        getCollectionHelper().count() == 3
    }

    @Category(Slow)
    def 'when documents together are just above the max message size, the documents are still inserted'() {
        given:
        def bsonBinary = new BsonBinary(new byte[16 * 1000 * 1000 - (getCollectionName().length() + 32)])
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [
                        new InsertRequest(new BsonDocument('_id', new BsonObjectId()).append('b', bsonBinary)),
                        new InsertRequest(new BsonDocument('_id', new BsonObjectId()).append('b', bsonBinary)),
                        new InsertRequest(new BsonDocument('_id', new BsonObjectId()).append('b', bsonBinary))
                ],
                true, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, true)

        then:
        result.wasAcknowledged()
        result.insertedCount == 3
        getCollectionHelper().count() == 3
    }


    def 'should handle multi-length runs of ordered insert, update, replace, and remove'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def operation = new MixedBulkWriteOperation(getNamespace(), getTestWrites(), ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

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
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    def 'should handle multi-length runs of UNACKNOWLEDGED insert, update, replace, and remove'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def operation = new MixedBulkWriteOperation(getNamespace(),  getTestWrites(), ordered, UNACKNOWLEDGED, false)
        def binding = async ? getAsyncSingleConnectionBinding() : getSingleConnectionBinding()

        when:
        def result = execute(operation, binding)
        execute(new InsertOperation(namespace, true, ACKNOWLEDGED, false,
                [new InsertRequest(new BsonDocument('_id', new BsonInt32(9)))]), binding)

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
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    @Category(Slow)
    def 'should split the number of writes is larger than the match write batch size'() {
        given:
        def binding = async ? getAsyncSingleConnectionBinding() : getSingleConnectionBinding()
        def maxWriteBatchSize = getCollectionHelper().isMaster().getInteger('maxWriteBatchSize').intValue()
        def numberOfWrites = maxWriteBatchSize + 100
        def writes = []

        (1..numberOfWrites).each {
            writes.add(new InsertRequest(new BsonDocument()))
        }
        def operation = new MixedBulkWriteOperation(getNamespace(), writes, ordered, ACKNOWLEDGED, false)

        when:
        execute(operation, binding)
        acknowledgeWrite(binding)

        then:
        getCollectionHelper().count() == numberOfWrites + 1

        where:
        [async, ordered, writeConcern] << [[true, false], [true, false], [ACKNOWLEDGED, UNACKNOWLEDGED]].combinations()
    }

    def 'should be able to merge upserts across batches'() {
        given:
        def writeOperations = []
        (0..1002).each {
            def upsert = new UpdateRequest(new BsonDocument('key', new BsonInt32(it)),
                                           new BsonDocument('$set', new BsonDocument('key', new BsonInt32(it))),
                        UPDATE).upsert(true)
            writeOperations.add(upsert)
            writeOperations.add(new DeleteRequest(new BsonDocument('key', new BsonInt32(it))))
        }
        def operation = new MixedBulkWriteOperation(getNamespace(), writeOperations, ordered, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result.deletedCount == result.upserts.size()
        getCollectionHelper().count() == 0

        where:
        [async, ordered] << [[false], [true]].combinations()
    }

    def 'error details should have correct index on ordered write failure'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3))),
                                                                UPDATE),
                                              new InsertRequest(new BsonDocument('_id', new BsonInt32(1))) // this should fail with index 2
                                             ], true, ACKNOWLEDGED, false)
        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 2
        ex.writeErrors[0].code == 11000

        where:
        async << [true, false]
    }

    def 'error details should have correct index on unordered write failure'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3))),
                                                                UPDATE),
                                              new InsertRequest(new BsonDocument('_id', new BsonInt32(3))) // this should fail with index 2
                                             ], false, ACKNOWLEDGED, false)
        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 2
        ex.writeErrors[0].index == 0
        ex.writeErrors[0].code == 11000
        ex.writeErrors[1].index == 2
        ex.writeErrors[1].code == 11000

        where:
        async << [true, false]
    }

    def 'should continue to execute batches after a failure if writes are unordered'() {
        given:
        getCollectionHelper().insertDocuments([new BsonDocument('_id', new BsonInt32(500)), new BsonDocument('_id', new BsonInt32(1500))])
        def inserts = []
        for (int i = 0; i < 2000; i++) {
            inserts.add(new InsertRequest(new BsonDocument('_id', new BsonInt32(i))))
        }
        def operation = new MixedBulkWriteOperation(getNamespace(), inserts, false, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 2
        ex.getWriteResult().getInsertedCount() == 1998
        getCollectionHelper().count() == 2000

        where:
        async << [true, false]
    }

    def 'should stop executing batches after a failure if writes are ordered'() {
        given:
        getCollectionHelper().insertDocuments([new BsonDocument('_id', new BsonInt32(500)), new BsonDocument('_id', new BsonInt32(1500))])
        def inserts = []
        for (int i = 0; i < 2000; i++) {
            inserts.add(new InsertRequest(new BsonDocument('_id', new BsonInt32(i))))
        }
        def operation = new MixedBulkWriteOperation(getNamespace(), inserts, true, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 1
        ex.getWriteResult().getInsertedCount() == 500
        getCollectionHelper().count() == 502


        where:
        async << [true, false]
    }

    // using w = 5 to force a timeout
    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'should throw bulk write exception with a write concern error when wtimeout is exceeded'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))],
                                             false, new WriteConcern(5, 1), false)
        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteConcernError() != null


        where:
        async << [true, false]
    }

    @IgnoreIf({ !isDiscoverableReplicaSet() })
    def 'when there is a duplicate key error and a write concern error, both should be reported'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def operation = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(7))),
                                              new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))   // duplicate key
                                             ], false, new WriteConcern(4, 1), false)

        when:
        execute(operation, async)  // This is assuming that it won't be able to replicate to 4 servers in 1 ms

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 1
        ex.writeErrors[0].code == 11000
        ex.writeConcernError != null

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 8) || !isDiscoverableReplicaSet() })
    def 'should throw on write concern error on multiple failpoint'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new InsertRequest(new BsonDocument('_id', new BsonInt32(7))),
                 new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))   // duplicate key
                ], false, ACKNOWLEDGED, true)

        def failPoint = BsonDocument.parse('''{
            "configureFailPoint": "failCommand",
            "mode": {"times": 2 },
            "data": { "failCommands": ["insert"],
                      "writeConcernError": {"code": 91, "errmsg": "Replication is being shut down"}}}''')
        configureFailPoint(failPoint)

        when:
        execute(operation, async)  // This is assuming that it won't be able to replicate to 4 servers in 1 ms

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 1
        ex.writeErrors[0].code == 11000
        ex.writeConcernError != null
        ex.writeConcernError.code == 91

        cleanup:
        disableFailPoint('failCommand')

        where:
        async << [true, false]
    }

    def 'should throw IllegalArgumentException when passed an empty bulk operation'() {
        when:
        new MixedBulkWriteOperation(getNamespace(), [], ordered, UNACKNOWLEDGED, false)

        then:
        thrown(IllegalArgumentException)

        where:
        ordered << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
    def 'should throw if bypassDocumentValidation is set and writeConcern is UNACKNOWLEDGED'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new InsertRequest(BsonDocument.parse('{ level: 9 }'))], true, UNACKNOWLEDGED, false)
                .bypassDocumentValidation(bypassDocumentValidation)

        when:
        execute(operation, async)

        then:
        thrown(MongoClientException)

        where:
        [async, bypassDocumentValidation] << [[false, false], [true, false]].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should throw if collation is set and write is UNACKNOWLEDGED'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new DeleteRequest(BsonDocument.parse('{ level: 9 }')).collation(defaultCollation)], true, UNACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        thrown(MongoClientException)

        where:
        [async, bypassDocumentValidation] << [[true, false], [true, false]].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
    def 'should honour the bypass validation flag for inserts'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collection')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create(namespace.getCollectionName(), new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))
        def operation = new MixedBulkWriteOperation(namespace, [new InsertRequest(BsonDocument.parse('{ level: 9 }'))], ordered,
                ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteErrors().get(0).code == 121

        when:
        operation.bypassDocumentValidation(true)
        BulkWriteResult result = execute(operation, async)

        then:
        notThrown(MongoBulkWriteException)
        result.wasAcknowledged()
        result.insertedCount == 1
        collectionHelper.count() == 1

        cleanup:
        collectionHelper?.drop()

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 2) })
    def 'should honour the bypass validation flag for updates'() {
        given:
        def namespace = new MongoNamespace(getDatabaseName(), 'collection')
        def collectionHelper = getCollectionHelper(namespace)
        collectionHelper.create(namespace.getCollectionName(), new CreateCollectionOptions().validationOptions(
                new ValidationOptions().validator(gte('level', 10))))

        collectionHelper.insertDocuments(BsonDocument.parse('{ x: true, level: 10}'))
        def operation = new MixedBulkWriteOperation(namespace,
                [new UpdateRequest(BsonDocument.parse('{x: true}'), BsonDocument.parse('{$inc: {level: -1}}'), UPDATE).multi(false)],
                ordered, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteErrors().get(0).code == 121

        when:
        operation.bypassDocumentValidation(true)
        BulkWriteResult result = execute(operation, async)

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [], [])
        collectionHelper.count(eq('level', 9)) == 1

        where:
        [async, ordered] << [[true, false], [true, false]].combinations()
    }

    @IgnoreIf({ serverVersionAtLeast(3, 4) })
    def 'should throw an exception when using an unsupported Collation'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', 1), new Document('y', 1),
                new Document('z', 1))
        def operation = new MixedBulkWriteOperation(namespace, requests, false, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        def exception = thrown(Exception)
        exception instanceof IllegalArgumentException
        exception.getMessage().startsWith('Collation not supported by wire version:')
        getCollectionHelper().count() == 3

        where:
        [async, requests] << [
                [true, false],
                [[new DeleteRequest(BsonDocument.parse('{x: 1}}')).collation(defaultCollation)],
                 [new UpdateRequest(BsonDocument.parse('{x: 1}}'), BsonDocument.parse('{x: 10}}'), REPLACE),
                  new UpdateRequest(BsonDocument.parse('{y: 1}}'), BsonDocument.parse('{x: 10}}'), REPLACE).collation(defaultCollation)],
                 [new DeleteRequest(BsonDocument.parse('{x: 1}}')),
                  new DeleteRequest(BsonDocument.parse('{y: 1}}')).collation(defaultCollation)],
                 [new DeleteRequest(BsonDocument.parse('{x: 1}}')),
                  new UpdateRequest(BsonDocument.parse('{y: 1}}'), BsonDocument.parse('{x: 10}}'), REPLACE).collation(defaultCollation)]]
        ].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) })
    def 'should support collation'() {
        given:
        getCollectionHelper().insertDocuments(Document.parse('{str: "foo"}'), Document.parse('{str: "bar"}'))
        def requests = [new DeleteRequest(BsonDocument.parse('{str: "FOO"}}')).collation(caseInsensitiveCollation),
                        new UpdateRequest(BsonDocument.parse('{str: "BAR"}}'), BsonDocument.parse('{str: "bar"}}'), REPLACE)
                                .collation(caseInsensitiveCollation)]
        def operation = new MixedBulkWriteOperation(namespace, requests, false, ACKNOWLEDGED, false)

        when:
        BulkWriteResult result = execute(operation, async)

        then:
        result.getDeletedCount() == 1
        result.getModifiedCount() == 1

        where:
        async << [true, false]
    }

    def 'should support retryWrites=true'() {
        given:
        def testWrites = getTestWrites()
        Collections.shuffle(testWrites)
        getCollectionHelper().insertDocuments(getTestInserts())
        def operation = new MixedBulkWriteOperation(getNamespace(), testWrites, true, ACKNOWLEDGED, true)

        when:
        if (serverVersionAtLeast(3, 6) && isDiscoverableReplicaSet()) {
            enableOnPrimaryTransactionalWriteFailPoint(BsonDocument.parse(failPoint))
        }
        BulkWriteResult result = executeWithSession(operation, async)

        then:
        result.wasAcknowledged()
        result.getInsertedCount() == 2
        result.getDeletedCount() == 2
        result.getMatchedCount() == 4
        result.getModifiedCount() == 4
        result.getUpserts().isEmpty()

        then:
        getCollectionHelper().find(new Document('_id', 1)).first() == new Document('_id', 1).append('x', 2)
        getCollectionHelper().find(new Document('_id', 2)).first() == new Document('_id', 2).append('x', 3)
        getCollectionHelper().find(new Document('_id', 3)).isEmpty()
        getCollectionHelper().find(new Document('_id', 4)).isEmpty()
        getCollectionHelper().find(new Document('_id', 5)).first() == new Document('_id', 5).append('x', 4)
        getCollectionHelper().find(new Document('_id', 6)).first() == new Document('_id', 6).append('x', 5)
        getCollectionHelper().find(new Document('_id', 7)).first() == new Document('_id', 7)
        getCollectionHelper().find(new Document('_id', 8)).first() == new Document('_id', 8)

        cleanup:
        disableOnPrimaryTransactionalWriteFailPoint()

        where:
        [async, ordered, failPoint] << [
            [true, false],
            [true, false],
            ['{mode: {times: 5}}', // SDAM will retry multiple times to find a server
             '{mode: {times: 1}, data: {failBeforeCommitExceptionCode : 1}}']
        ].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) || !isDiscoverableReplicaSet() })
    def 'should fail as expected with retryWrites and failPoints'() {
        given:
        def testWrites = getTestWrites()
        getCollectionHelper().insertDocuments(getTestInserts())
        def operation = new MixedBulkWriteOperation(getNamespace(), testWrites, true, ACKNOWLEDGED, true)

        when:
        enableOnPrimaryTransactionalWriteFailPoint(BsonDocument.parse(failPoint))
        executeWithSession(operation, async)

        then:
        thrown(MongoSocketReadException)

        cleanup:
        disableOnPrimaryTransactionalWriteFailPoint()

        where:
        [async, failPoint] << [
                [true, false],
                ['{mode: {times: 2}, data: {failBeforeCommitExceptionCode : 1}}',
                 '{mode: {skip: 2}, data: {failBeforeCommitExceptionCode : 1}}']
        ].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) || !isDiscoverableReplicaSet() })
    def 'should not fail with unacknowledged writes, retryWrites and failPoints'() {
        given:
        def testWrites = getTestWrites()
        getCollectionHelper().insertDocuments(getTestInserts())
        def operation = new MixedBulkWriteOperation(getNamespace(), testWrites, true, UNACKNOWLEDGED, true)

        when:
        enableOnPrimaryTransactionalWriteFailPoint(BsonDocument.parse(failPoint))
        def result = executeWithSession(operation, async)

        then:
        result == BulkWriteResult.unacknowledged()

        cleanup:
        disableOnPrimaryTransactionalWriteFailPoint()

        where:
        [async, failPoint] << [
                [true, false],
                ['{mode: {times: 2}, data: {failBeforeCommitExceptionCode : 1}}',
                 '{mode: {skip: 2}, data: {failBeforeCommitExceptionCode : 1}}']
        ].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should retry if the connection initially fails'() {
        when:
        def cannedResult = BsonDocument.parse('{ok: 1.0, n: 1}')
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new InsertRequest(BsonDocument.parse('{ level: 9 }'))], true, ACKNOWLEDGED, true)
        def expectedCommand = new BsonDocument('insert', new BsonString(getNamespace().getCollectionName()))
                .append('ordered', BsonBoolean.TRUE)
                .append('txnNumber', new BsonInt64(0))

        then:
        testOperationRetries(operation, [3, 6, 0], expectedCommand, async, cannedResult)

        where:
        async << [true, false]
    }

    def 'should throw original error when retrying and failing'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(),
                [new InsertRequest(BsonDocument.parse('{ level: 9 }'))], true, ACKNOWLEDGED, true)
        def originalException = new MongoSocketException('Some failure', new ServerAddress())

        when:
        testRetryableOperationThrowsOriginalError(operation, [[3, 6, 0], [3, 6, 0], [3, 4, 0]],
                [REPLICA_SET_PRIMARY, REPLICA_SET_PRIMARY], originalException, async)

        then:
        Exception commandException = thrown()
        commandException == originalException

        when:
        testRetryableOperationThrowsOriginalError(operation, [[3, 6, 0], [3, 6, 0], [3, 6, 0]],
                [REPLICA_SET_PRIMARY, STANDALONE], originalException, async)

        then:
        commandException = thrown()
        commandException == originalException

        when:
        testRetryableOperationThrowsOriginalError(operation, [[3, 6, 0], [3, 6, 0]],
                [REPLICA_SET_PRIMARY], originalException, async)

        then:
        commandException = thrown()
        commandException == originalException

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should not request retryable write for multi updates or deletes'() {
        given:
        def operation = new MixedBulkWriteOperation(getNamespace(), writes, true, ACKNOWLEDGED, true)

        when:
        executeWithSession(operation, async)

        then:
        noExceptionThrown()

        where:
        [async, writes] << [
                [true, false],
                // Test scenarios where the multi:true request is at the beginning and at the end of the list
                [
                        [
                                new DeleteRequest(new BsonDocument()).multi(true),
                                new InsertRequest(new BsonDocument())
                        ],
                        [
                                new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                        new BsonDocument('$set', new BsonDocument('_id', new BsonInt32(1))), UPDATE).multi(true),
                                new InsertRequest(new BsonDocument())
                        ],
                        [
                                new InsertRequest(new BsonDocument()),
                                new DeleteRequest(new BsonDocument()).multi(true)
                        ],
                        [
                                new InsertRequest(new BsonDocument()),
                                new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                        new BsonDocument('$set', new BsonDocument('_id', new BsonInt32(1))), UPDATE).multi(true)
                        ]
                ]
        ].combinations()
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should support array filters'() {
        given:
        def documentOne = BsonDocument.parse('{_id: 1, y: [ {b: 3}, {b: 1}]}')
        def documentTwo = BsonDocument.parse('{_id: 2, y: [ {b: 0}, {b: 1}]}')
        getCollectionHelper().insertDocuments(documentOne, documentTwo)
        def requests = [
                new UpdateRequest(new BsonDocument(), BsonDocument.parse('{ $set: {"y.$[i].b": 2}}'), UPDATE)
                        .arrayFilters([BsonDocument.parse('{"i.b": 3}')]),
                new UpdateRequest(new BsonDocument(), BsonDocument.parse('{ $set: {"y.$[i].b": 4}}'), UPDATE)
                        .multi(true)
                        .arrayFilters([BsonDocument.parse('{"i.b": 1}')]),
        ]
        def operation = new MixedBulkWriteOperation(namespace, requests, true, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        getCollectionHelper().find(new BsonDocumentCodec()) == [
                BsonDocument.parse('{_id: 1, y: [ {b: 2}, {b: 4}]}'),
                BsonDocument.parse('{_id: 2, y: [ {b: 0}, {b: 4}]}')
        ]

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionAtLeast(3, 6) })
    def 'should throw IllegalArgumentException if array filters is set and server version is less than 3.6'() {
        given:
        def requests = [
                new UpdateRequest(new BsonDocument(), BsonDocument.parse('{ $set: {"y.$[i].b": 2}}'), UPDATE)
                        .arrayFilters([BsonDocument.parse('{"i.b": 3}')])
        ]
        def operation = new MixedBulkWriteOperation(namespace, requests, true, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        thrown(IllegalArgumentException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 6) })
    def 'should throw if array filters is set and write concern is UNACKNOWLEDGED'() {
        given:
        def requests = [
                new UpdateRequest(new BsonDocument(), BsonDocument.parse('{ $set: {"y.$[i].b": 2}}'), UPDATE)
                        .arrayFilters([BsonDocument.parse('{"i.b": 3}')])
        ]
        def operation = new MixedBulkWriteOperation(namespace, requests, true, UNACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        thrown(MongoClientException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ serverVersionAtLeast(3, 4) })
    def 'should throw IllegalArgumentException if hint is set and server version is less than 3.4'() {
        given:
        def requests = [
                new UpdateRequest(new BsonDocument(), BsonDocument.parse('{ $set: {"y.$[i].b": 2}}'), UPDATE)
                        .hint(BsonDocument.parse('{ _id: 1 }'))
        ]
        def operation = new MixedBulkWriteOperation(namespace, requests, true, ACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        thrown(IllegalArgumentException)

        where:
        async << [true, false]
    }

    @IgnoreIf({ !serverVersionAtLeast(3, 4) || serverVersionAtLeast(4, 2) })
    def 'should throw if hint is set and write concern is UNACKNOWLEDGED'() {
        given:
        def requests = [
                new UpdateRequest(new BsonDocument(), BsonDocument.parse('{ $set: {"y.$[i].b": 2}}'), UPDATE)
                        .hintString('_id')
        ]
        def operation = new MixedBulkWriteOperation(namespace, requests, true, UNACKNOWLEDGED, false)

        when:
        execute(operation, async)

        then:
        thrown(MongoClientException)

        where:
        async << [true, false]
    }

    private static List<WriteRequest> getTestWrites() {
        [new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                           new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))),
                           UPDATE).multi(false),
         new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                           new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3))),
                           UPDATE).multi(false),
         new DeleteRequest(new BsonDocument('_id', new BsonInt32(3))).multi(false),
         new DeleteRequest(new BsonDocument('_id', new BsonInt32(4))).multi(false),
         new UpdateRequest(new BsonDocument('_id', new BsonInt32(5)),
                           new BsonDocument('_id', new BsonInt32(5)).append('x', new BsonInt32(4)),
                           REPLACE).multi(false),
         new UpdateRequest(new BsonDocument('_id', new BsonInt32(6)),
                           new BsonDocument('_id', new BsonInt32(6)).append('x', new BsonInt32(5)),
                           REPLACE).multi(false),
         new InsertRequest(new BsonDocument('_id', new BsonInt32(7))),
         new InsertRequest(new BsonDocument('_id', new BsonInt32(8)))
        ]
    }

    private static BsonDocument[] getTestInserts() {
        [new BsonDocument('_id', new BsonInt32(1)),
         new BsonDocument('_id', new BsonInt32(2)),
         new BsonDocument('_id', new BsonInt32(3)),
         new BsonDocument('_id', new BsonInt32(4)),
         new BsonDocument('_id', new BsonInt32(5)),
         new BsonDocument('_id', new BsonInt32(6))]
    }

    private static Integer expectedModifiedCount(final int expectedCountForServersThatSupportIt) {
        expectedCountForServersThatSupportIt
    }
}
