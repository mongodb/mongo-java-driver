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

import category.Slow
import com.mongodb.ClusterFixture
import com.mongodb.MongoBulkWriteException
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ReadPreference
import com.mongodb.WriteConcern
import com.mongodb.binding.SingleConnectionBinding
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.BulkWriteUpsert
import com.mongodb.bulk.DeleteRequest
import com.mongodb.bulk.InsertRequest
import com.mongodb.bulk.UpdateRequest
import com.mongodb.bulk.WriteRequest
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

import static ClusterFixture.getBinding
import static ClusterFixture.getSingleConnectionBinding
import static ClusterFixture.serverVersionAtLeast
import static WriteConcern.ACKNOWLEDGED
import static WriteConcern.UNACKNOWLEDGED
import static com.mongodb.ClusterFixture.getCluster
import static com.mongodb.bulk.WriteRequest.Type.DELETE
import static com.mongodb.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.bulk.WriteRequest.Type.UPDATE

class MixedBulkWriteOperationSpecification extends OperationFunctionalSpecification {

    def 'when no document with the same id exists, should insert the document'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))], ordered,
                                             ACKNOWLEDGED)

        when:
        def result = op.execute(getBinding())

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
        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest(document)], ordered,
                                             ACKNOWLEDGED)

        when:
        op.execute(getBinding())

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
        def result = op.execute(getBinding())

        then:
        result == BulkWriteResult.acknowledged(DELETE, 1, [])
        getCollectionHelper().count() == 1

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, a remove should remove all of them'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('x', true), new Document('x', true),
                                              new Document('x', false))

        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new DeleteRequest(new BsonDocument('x', BsonBoolean.TRUE))],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = op.execute(getBinding())

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
        def result = op.execute(getBinding())

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
        def result = op.execute(getBinding())

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
        def result = op.execute(getBinding())

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
        def result = op.execute(getBinding())

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
        def result = op.execute(getBinding())

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
        def result = op.execute(getBinding())

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
        op.execute(getBinding())

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
        op.execute(getBinding())

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
        op.execute(getBinding())

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
                                                                new BsonDocument('_id', new BsonObjectId(id))
                                                                        .append('x', new BsonInt32(2)),
                                                                REPLACE)
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED)

        when:
        def result = op.execute(getBinding())

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
        def result = op.execute(getBinding())

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
        def binding = new SingleConnectionBinding(getCluster(), ReadPreference.primary())
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
        def result = op.execute(binding)
        getCollectionHelper().insertDocuments(new DocumentCodec(), binding, new Document('_id', 4))

        then:
        !result.wasAcknowledged()
        getCollectionHelper().count() == 4

        cleanup:
        binding?.release()

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
        def result = op.execute(getBinding())

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count(new Document('x', false)) == 1

        where:
        ordered << [true, false]
    }

    @Category(Slow)
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
        def result = op.execute(getBinding())

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count() == 1
    }

    @Category(Slow)
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
        def result = op.execute(getBinding())

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 2, expectedModifiedCount(2), [])
        getCollectionHelper().count() == 2
    }

    def 'should handle multi-length runs of ordered insert, update, replace, and remove'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             getTestWrites(), ordered,
                                             ACKNOWLEDGED)

        when:
        def result = op.execute(getBinding())

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
        getCollectionHelper().insertDocuments(getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             getTestWrites(), ordered,
                                             UNACKNOWLEDGED)

        when:
        def binding = getSingleConnectionBinding()
        def result = op.execute(binding)
        new InsertOperation(namespace, true, ACKNOWLEDGED, [new InsertRequest(new BsonDocument('_id', new BsonInt32(9)))])
                .execute(binding);

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
        new MixedBulkWriteOperation(getNamespace(), writes, ordered, ACKNOWLEDGED).execute(getBinding())

        then:
        getCollectionHelper().count() == 2001

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
        op.execute(getBinding())

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 2
        ex.writeErrors[0].code == 11000
    }

    def 'error details should have correct index on unordered write failure'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3))),
                                                                UPDATE),
                                              new InsertRequest(new BsonDocument('_id', new BsonInt32(3))) // this should fail with index 2
                                             ], false, ACKNOWLEDGED)
        when:
        op.execute(getBinding())

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 2
        ex.writeErrors[0].index == 0
        ex.writeErrors[0].code == 11000
        ex.writeErrors[1].index == 2
        ex.writeErrors[1].code == 11000
    }

    def 'should continue to execute batches after a failure if writes are unordered'() {
        given:
        getCollectionHelper().insertDocuments([new BsonDocument('_id', new BsonInt32(500)), new BsonDocument('_id', new BsonInt32(1500))])
        def inserts = []
        for (int i = 0; i < 2000; i++) {
            inserts.add(new InsertRequest(new BsonDocument('_id', new BsonInt32(i))))
        }
        def op = new MixedBulkWriteOperation(getNamespace(), inserts, false, ACKNOWLEDGED)

        when:
        op.execute(getBinding())

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 2
        ex.getWriteResult().getInsertedCount() == 1998
        getCollectionHelper().count() == 2000
    }

    def 'should stop executing batches after a failure if writes are ordered'() {
        given:
        getCollectionHelper().insertDocuments([new BsonDocument('_id', new BsonInt32(500)), new BsonDocument('_id', new BsonInt32(1500))])
        def inserts = []
        for (int i = 0; i < 2000; i++) {
            inserts.add(new InsertRequest(new BsonDocument('_id', new BsonInt32(i))))
        }
        def op = new MixedBulkWriteOperation(getNamespace(), inserts, true, ACKNOWLEDGED)

        when:
        op.execute(getBinding())

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.writeErrors.size() == 1
        ex.getWriteResult().getInsertedCount() == 500
        getCollectionHelper().count() == 502
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
        op.execute(getBinding())

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteConcernError() != null
    }

    @IgnoreIf({ !ClusterFixture.isDiscoverableReplicaSet() })
    def 'when there is a duplicate key error and a write concern error, both should be reported'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest(new BsonDocument('_id', new BsonInt32(7))),
                                              new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))   // duplicate key
                                             ], ordered, new WriteConcern(4, 1))

        when:
        op.execute(getBinding())  // This is assuming that it won't be able to replicate to 4 servers in 1 ms

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

    private static BsonDocument[] getTestInserts() {
        [new BsonDocument('_id', new BsonInt32(1)),
         new BsonDocument('_id', new BsonInt32(2)),
         new BsonDocument('_id', new BsonInt32(3)),
         new BsonDocument('_id', new BsonInt32(4)),
         new BsonDocument('_id', new BsonInt32(5)),
         new BsonDocument('_id', new BsonInt32(6))]
    }

    private static Integer expectedModifiedCount(final int expectedCountForServersThatSupportIt) {
        (serverVersionAtLeast([2, 6, 0])) ? expectedCountForServersThatSupportIt : null
    }
}
