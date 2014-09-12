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
import com.mongodb.OperationFunctionalSpecification
import com.mongodb.WriteConcern
import com.mongodb.codecs.DocumentCodec
import com.mongodb.protocol.AcknowledgedBulkWriteResult
import org.bson.BsonBoolean
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.types.Binary
import org.bson.types.ObjectId
import org.junit.experimental.categories.Category
import org.mongodb.BulkWriteException
import org.mongodb.BulkWriteUpsert
import org.mongodb.Document
import spock.lang.IgnoreIf

import static ClusterFixture.getAsyncBinding
import static ClusterFixture.getAsyncSingleConnectionBinding
import static ClusterFixture.serverVersionAtLeast
import static WriteConcern.ACKNOWLEDGED
import static WriteConcern.UNACKNOWLEDGED
import static com.mongodb.operation.WriteRequest.Type.REMOVE
import static com.mongodb.operation.WriteRequest.Type.UPDATE

@Category(Async)
class MixedBulkWriteOperationAsyncSpecification extends OperationFunctionalSpecification {

    def 'when no document with the same id exists, should insert the document'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest<Document>(new Document('_id', 1))], ordered,
                                             ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result.insertedCount == 1
        result.upserts == []
        getCollectionHelper().count() == 1

        where:
        ordered << [true, false]
    }

    def 'when a document with the same id exists, should throw an exception'() {
        given:
        def document = new Document('_id', 1)
        getCollectionHelper().insertDocuments(document)
        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest<Document>(document)], ordered,
                                             ACKNOWLEDGED, new DocumentCodec())

        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        def ex = thrown(BulkWriteException)
        ex.getWriteErrors().get(0).code == 11000

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, a remove of one should remove one of them'() {
        given:
        getCollectionHelper().insertDocuments(new Document('x', true), new Document('x', true))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new RemoveRequest(new BsonDocument('x', BsonBoolean.TRUE)).multi(false)],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(REMOVE, 1, [])
        getCollectionHelper().count() == 1

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, a remove should remove all of them'() {
        given:
        getCollectionHelper().insertDocuments(new Document('x', true), new Document('x', true), new Document('x', false))

        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new RemoveRequest(new BsonDocument('x', BsonBoolean.TRUE))],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(REMOVE, 2, [])
        getCollectionHelper().count() == 1

        where:
        ordered << [true, false]
    }

    def 'when multiple document match the query, update of one should update only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new Document('x', true), new Document('x', true));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))))],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count(new Document('y', 1)) == 1

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, update multi should update all of them'() {
        given:
        getCollectionHelper().insertDocuments(new Document('x', true), new Document('x', true));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))))
                                                      .multi(true)],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 2, expectedModifiedCount(2), [])
        getCollectionHelper().count(new Document('y', 1)) == 2

        where:
        ordered << [true, false]
    }

    def 'when no document matches the query, an update of one with upsert should insert a document'() {
        def id = new ObjectId()
        def query = new BsonDocument('_id', new BsonObjectId(id))
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(query, new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))))
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonObjectId(id))])
        getCollectionHelper().find().first() == new Document('_id', query.getObjectId('_id').getValue()).append('x', 2)

        where:
        ordered << [true, false]
    }

    def 'when no document matches the query, an update multi with upsert should insert a document'() {
        def id = new ObjectId()
        def query = new BsonDocument('_id', new BsonObjectId(id))
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(query, new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))))
                                                      .upsert(true).multi(true)],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonObjectId(id))])
        getCollectionHelper().find().first() == new Document('_id', query.getObjectId('_id').getValue()).append('x', 2)

        where:
        ordered << [true, false]
    }

    def 'when documents matches the query, update one with upsert should update only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new Document('x', true), new Document('x', true));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))))
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count(new Document('y', 1)) == 1

        where:
        ordered << [true, false]
    }

    def 'when documents match the query, update multi with upsert should update all of them'() {
        given:
        getCollectionHelper().insertDocuments(new Document('x', true), new Document('x', true));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                new BsonDocument('$set', new BsonDocument('y', new BsonInt32(1))))
                                                      .upsert(true).multi(true)],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 2, expectedModifiedCount(2), [])
        getCollectionHelper().count(new Document('y', 1)) == 2

        where:
        ordered << [true, false]
    }

    def 'when a document contains a key with an illegal character, replacing a document with it should throw IllegalArgumentException'() {
        given:
        def id = new ObjectId()
        def encoder = new DocumentCodec();
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new ReplaceRequest(new BsonDocument('_id', new BsonObjectId(id)),
                                                                 new Document('$set', new Document('x', 1)))
                                                      .upsert(true)],
                                             true, ACKNOWLEDGED, encoder)

        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        thrown(IllegalArgumentException)

        where:
        ordered << [true, false]
    }

    def 'when no document matches the query, a replace with upsert should insert a document'() {
        given:
        def id = new ObjectId()
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new ReplaceRequest(new BsonDocument('_id', new BsonObjectId(id)),
                                                                 new Document('_id', id).append('x', 2))
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonObjectId(id))])
        getCollectionHelper().find().first() == new Document('_id', id).append('x', 2)

        where:
        ordered << [true, false]
    }

    def 'when a custom _id is upserted it should be in the write result'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new BsonDocument('_id', new BsonInt32(0)),
                                                                new BsonDocument('$set', new BsonDocument('a', new BsonInt32(0))))
                                                      .upsert(true),
                                              new ReplaceRequest(new BsonDocument('a', new BsonInt32(1)), new Document('_id', 1))
                                                      .upsert(true),
                                              new ReplaceRequest(new BsonDocument('_id', new BsonInt32(2)), new Document('_id', 2))
                                                      .upsert(true)
                                             ],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 0, expectedModifiedCount(0), [new BulkWriteUpsert(0, new BsonInt32(0)),
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
                                                                new BsonDocument('$set', new BsonDocument('a', new BsonInt32(0))))
                                                      .upsert(true),
                                              new ReplaceRequest(new BsonDocument('a', new BsonInt32(1)), new Document('_id', 1))
                                                      .upsert(true),
                                              new ReplaceRequest(new BsonDocument('_id', new BsonInt32(2)), new Document('_id', 2))
                                                      .upsert(true)
                                             ],
                                             ordered, UNACKNOWLEDGED, new DocumentCodec())

        when:
        def binding = getAsyncSingleConnectionBinding()
        def result = op.executeAsync(binding).get()

        then:
        !result.acknowledged
        acknowledgeWrite(binding)
        getCollectionHelper().count() == 4

        where:
        ordered << [true, false]
    }

    def 'when multiple documents match the query, replace should replace only one of them'() {
        given:
        getCollectionHelper().insertDocuments(new Document('x', true), new Document('x', true))

        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new ReplaceRequest(new BsonDocument('x', BsonBoolean.TRUE),
                                                                 new Document('y', 1).append('x', false))
                                                      .upsert(true)],
                                             ordered, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count(new Document('x', false)) == 1

        where:
        ordered << [true, false]
    }

    @Category([Async, Slow])
    def 'when a replacement document is 16MB, the document is still replaced'() {
        given:
        getCollectionHelper().insertDocuments(new Document('_id', 1))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new ReplaceRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                 new Document('_id', 1)
                                                                         .append('x', new Binary(new byte[1024 * 1024 * 16 - 30])))
                                                      .upsert(true)],
                                             true, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 1, expectedModifiedCount(1), [])
        getCollectionHelper().count() == 1
    }

    @Category([Async, Slow])
    def 'when two update documents together exceed 16MB, the documents are still updated'() {
        given:
        getCollectionHelper().insertDocuments(new Document('_id', 1), new Document('_id', 2))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new ReplaceRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                 new Document('_id', 1)
                                                                         .append('x', new Binary(new byte[1024 * 1024 * 16 - 30]))),
                                              new ReplaceRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                 new Document('_id', 2)
                                                                         .append('x', new Binary(new byte[1024 * 1024 * 16 - 30])))],
                                             true, ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 2, expectedModifiedCount(2), [])
        getCollectionHelper().count() == 2
    }

    def 'should handle multi-length runs of ordered insert, update, replace, and remove'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             getTestWrites(), ordered,
                                             ACKNOWLEDGED, new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result.acknowledged
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
                                             UNACKNOWLEDGED, new DocumentCodec())

        when:
        def binding = getAsyncSingleConnectionBinding()
        def result = op.executeAsync(binding).get()

        then:
        !result.acknowledged
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
            writes.add(new InsertRequest(new Document()))
        }

        when:
        new MixedBulkWriteOperation(getNamespace(), writes, ordered, ACKNOWLEDGED, new DocumentCodec())
                .executeAsync(getAsyncBinding())
                .get()

        then:
        getCollectionHelper().count() == 2001

        where:
        ordered << [true, false]
    }

    def 'error details should have correct index on ordered write failure'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest<Document>(new Document('_id', 1)),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3)))),
                                              new InsertRequest<Document>(new Document('_id', 1))   // this should fail with index 2
                                             ], true, ACKNOWLEDGED, new DocumentCodec())
        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        def ex = thrown(BulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 2
        ex.writeErrors[0].code == 11000
    }

    def 'error details should have correct index on unordered write failure'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest<Document>(new Document('_id', 1)),
                                              new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3)))),
                                              new InsertRequest<Document>(new Document('_id', 3))   // this should fail with index 2
                                             ], false, ACKNOWLEDGED, new DocumentCodec())
        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        def ex = thrown(BulkWriteException)
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
                                             [new InsertRequest<Document>(new Document('_id', 1))],
                                             false, new WriteConcern(5, 1), new DocumentCodec()
        )
        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        def ex = thrown(BulkWriteException)
        ex.getWriteConcernError() != null
    }

    @IgnoreIf({ !ClusterFixture.isDiscoverableReplicaSet() })
    def 'when there is a duplicate key error and a write concern error, both should be reported'() {
        given:
        getCollectionHelper().insertDocuments(getTestInserts())
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest<Document>(new Document('_id', 7)),
                                              new InsertRequest<Document>(new Document('_id', 1))   // duplicate key
                                             ], ordered, new WriteConcern(4, 1), new DocumentCodec())

        when:
        op.executeAsync(getAsyncBinding()).get()  // This is assuming that it won't be able to replicate to 4 servers in 1 ms

        then:
        def ex = thrown(BulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 1
        ex.writeErrors[0].code == 11000
        ex.writeConcernError != null

        where:
        ordered << [false]
    }

    def 'execute should throw IllegalStateException when already executed'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest<Document>(new Document('_id', 1))],
                                             ordered, UNACKNOWLEDGED, new DocumentCodec())

        op.executeAsync(getAsyncBinding()).get()

        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        thrown(IllegalStateException)

        where:
        ordered << [true, false]
    }

    def 'should throw IllegalArgumentException when passed an empty bulk operation'() {

        when:
        new MixedBulkWriteOperation(getNamespace(), [], ordered, UNACKNOWLEDGED, new DocumentCodec())

        then:
        thrown(IllegalArgumentException)

        where:
        ordered << [true, false]
    }

    private static List<WriteRequest> getTestWrites() {
        [new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                           new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2)))),
         new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                           new BsonDocument('$set', new BsonDocument('x', new BsonInt32(3)))),
         new RemoveRequest(new BsonDocument('_id', new BsonInt32(3))),
         new RemoveRequest(new BsonDocument('_id', new BsonInt32(4))),
         new ReplaceRequest(new BsonDocument('_id', new BsonInt32(5)),
                            new Document('_id', 5).append('x', 4)),
         new ReplaceRequest(new BsonDocument('_id', new BsonInt32(6)),
                            new Document('_id', 6).append('x', 5)),
         new InsertRequest<Document>(new Document('_id', 7)),
         new InsertRequest<Document>(new Document('_id', 8))
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
