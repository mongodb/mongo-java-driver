/*
 * Copyright (c) 2008 - 2014 MongoDB Inc. <http://mongodb.com>
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







package org.mongodb.operation

import org.bson.types.ObjectId
import org.mongodb.BulkWriteException
import org.mongodb.BulkWriteUpsert
import org.mongodb.Document
import org.mongodb.Fixture
import org.mongodb.FunctionalSpecification
import org.mongodb.WriteConcern
import org.mongodb.codecs.DocumentCodec
import org.mongodb.protocol.AcknowledgedBulkWriteResult

import static java.util.Arrays.asList
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getSession
import static org.mongodb.WriteConcern.ACKNOWLEDGED
import static org.mongodb.WriteConcern.UNACKNOWLEDGED
import static org.mongodb.operation.WriteRequest.Type.REMOVE
import static org.mongodb.operation.WriteRequest.Type.UPDATE

class BulkWriteOperationSpecification extends FunctionalSpecification {

    def 'when no document with the same id exists, should insert the document'() {
        given:

        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest<Document>(new Document('_id', 1))], true,
                                             ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result.insertedCount == 1
        result.upserts == []
        collection.find().count() == 1
    }

    def 'when a document with the same id exists, should throw an exception'() {
        given:
        def document = new Document('_id', 1)
        collection.insert(document)
        def op = new MixedBulkWriteOperation(getNamespace(), [new InsertRequest<Document>(document)], true,
                                             ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        op.execute()

        then:
        def ex = thrown(BulkWriteException)
        ex.getWriteErrors().get(0).code == 11000
    }

    def 'when documents match the query, a remove of one should remove one of them'() {
        given:
        collection.insert(new Document('x', true))
        collection.insert(new Document('x', true))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new RemoveRequest(new Document('x', true)).multi(false)],
                                             true, ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(REMOVE, 1, [])
        collection.find().count() == 1
    }

    def 'when documents match the query, a remove should remove all of them'() {
        given:
        collection.insert(asList(new Document('x', true), new Document('x', true), new Document('x', false)));

        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new RemoveRequest(new Document('x', true))],
                                             true, ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(REMOVE, 2, [])
        collection.find().count() == 1
    }

    def 'when a document matches the query, an update of one should update that document'() {
        given:
        collection.insert(new Document('_id', 1));
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new Document('_id', 1),
                                                                new Document('$set', new Document('x', 2))).upsert(true)],
                                             true, ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 1, 1, [])
        collection.find().one == new Document('_id', 1).append('x', 2)
    }

    def 'when documents match the query, an update should update all of them'() {
        given:
        collection.insert(new Document('x', true))
        collection.insert(new Document('x', true))
        collection.insert(new Document('x', false))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new Document('x', true),
                                                                new Document('$set', new Document('y', 1))).multi(true)],
                                             true, ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 2, 2, [])
        collection.find(new Document('y', 1)).count() == 2
    }

    def 'when no document matches the query, an update with upsert should insert a document'() {
        def id = new ObjectId()
        def query = new Document('_id', id)
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(query, new Document('$set', new Document('x', 2))).upsert(true)],
                                             true, ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 0, [new BulkWriteUpsert(0, id)])
        collection.find().one == new Document('_id', query.get('_id')).append('x', 2)
    }

    def 'when no document matches the query, a replace with upsert should insert a document'() {
        given:
        def id = new ObjectId()
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new ReplaceRequest(new Document('_id', id), new Document('_id', id).append('x', 2))
                                                      .upsert(true)
                                             ],
                                             true,
                                             ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 0, [new BulkWriteUpsert(0, id)])
        collection.find().one == new Document('_id', id).append('x', 2)
    }

    def 'when a document matches the query, an update with upsert should update that document'() {
        given:
        def id = new ObjectId()
        collection.insert(new Document('_id', id))

        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new UpdateRequest(new Document('_id', id),
                                                                new Document('$set', new Document('x', 2))).upsert(true)],
                                             true, ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 1, 1, [])
        collection.find().one == new Document('_id', id).append('x', 2)
    }

    def 'when a document matches the query, a replace with upsert should update that document'() {
        given:
        collection.insert(new Document('_id', 1))
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new ReplaceRequest(new Document('_id', 1), new Document('_id', 1).append('x', 2))
                                                      .upsert(true)], true,
                                             ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result.updatedCount == 1
        result.upserts == []
        collection.find().one == new Document('_id', 1).append('x', 2)
    }

    def 'should handle multi-length runs of ordered insert, update, replace, and remove'() {
        given:

        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                      getTestInserts(),
                                      new DocumentCodec(), getBufferProvider(), getSession(),
                                      true).execute()
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             getTestWrites(), true,
                                             ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(2, 4, 2, 4, [])
        collection.find(new Document('_id', 1)).one == new Document('_id', 1).append('x', 2)
        collection.find(new Document('_id', 2)).one == new Document('_id', 2).append('x', 3)
        collection.find(new Document('_id', 3)).one == null
        collection.find(new Document('_id', 4)).one == null
        collection.find(new Document('_id', 5)).one == new Document('_id', 5).append('x', 4)
        collection.find(new Document('_id', 6)).one == new Document('_id', 6).append('x', 5)
        collection.find(new Document('_id', 7)).one == new Document('_id', 7)
        collection.find(new Document('_id', 8)).one == new Document('_id', 8)
    }

    def 'should handle multi-length runs of unacknowledged insert, update, replace, and remove'() {
        given:

        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                      getTestInserts(),
                                      new DocumentCodec(), getBufferProvider(), getSession(),
                                      true).execute()
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             getTestWrites(), true,
                                             UNACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        !result.acknowledged
        collection.find(new Document('_id', 1)).one == new Document('_id', 1).append('x', 2)
        collection.find(new Document('_id', 2)).one == new Document('_id', 2).append('x', 3)
        collection.find(new Document('_id', 3)).one == null
        collection.find(new Document('_id', 4)).one == null
        collection.find(new Document('_id', 5)).one == new Document('_id', 5).append('x', 4)
        collection.find(new Document('_id', 6)).one == new Document('_id', 6).append('x', 5)
        collection.find(new Document('_id', 7)).one == new Document('_id', 7)
        collection.find(new Document('_id', 8)).one == new Document('_id', 8)
    }

    def 'error details should have correct index on ordered write failure'() {
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest<Document>(new Document('_id', 1)),
                                              new UpdateRequest(new Document('_id', 1), new Document('$set', new Document('x', 3))),
                                              new InsertRequest<Document>(new Document('_id', 1))   // this should fail with index 2
                                             ],
                                             true, ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)
        when:
        op.execute()

        then:
        def ex = thrown(BulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 2
        ex.writeErrors[0].code == 11000
    }

    def 'should handle multi-length runs of unordered insert, update, replace, and remove'() {
        given:

        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                      getTestInserts(),
                                      new DocumentCodec(), getBufferProvider(), getSession(),
                                      true).execute()
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             getTestWrites(), false,
                                             ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)

        when:
        def result = op.execute()

        then:
        result == new AcknowledgedBulkWriteResult(2, 4, 2, 4, [])
        collection.find(new Document('_id', 1)).one == new Document('_id', 1).append('x', 2)
        collection.find(new Document('_id', 2)).one == new Document('_id', 2).append('x', 3)
        collection.find(new Document('_id', 3)).one == null
        collection.find(new Document('_id', 4)).one == null
        collection.find(new Document('_id', 5)).one == new Document('_id', 5).append('x', 4)
        collection.find(new Document('_id', 6)).one == new Document('_id', 6).append('x', 5)
        collection.find(new Document('_id', 7)).one == new Document('_id', 7)
        collection.find(new Document('_id', 8)).one == new Document('_id', 8)
    }

    def 'error details should have correct index on unordered write failure'() {
        given:
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED,
                                      getTestInserts(),
                                      new DocumentCodec(), getBufferProvider(), getSession(),
                                      true).execute()
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest<Document>(new Document('_id', 1)),
                                              new UpdateRequest(new Document('_id', 2), new Document('$set', new Document('x', 3))),
                                              new InsertRequest<Document>(new Document('_id', 3))   // this should fail with index 2
                                             ],
                                             false, ACKNOWLEDGED, new DocumentCodec(), getBufferProvider(), getSession(), true)
        when:
        op.execute()

        then:
        def ex = thrown(BulkWriteException)
        ex.writeErrors.size() == 2
        ex.writeErrors[0].index == 0
        ex.writeErrors[0].code == 11000
        ex.writeErrors[1].index == 2
        ex.writeErrors[1].code == 11000
    }

    def 'should throw bulk write exception with a write concern error'() {
        assumeTrue(Fixture.isDiscoverableReplicaSet())
        given:
        def op = new MixedBulkWriteOperation(getNamespace(),
                                             [new InsertRequest<Document>(new Document('_id', 1))],
                                             false, new WriteConcern(2, 1), new DocumentCodec(), getBufferProvider(), getSession(),
                                             true)
        when:
        op.execute()

        then:
        def ex = thrown(BulkWriteException)
        ex.getWriteConcernError() != null

    }

    private static List<WriteRequest> getTestWrites() {
        [new UpdateRequest(new Document('_id', 1),
                           new Document('$set', new Document('x', 2))),
         new UpdateRequest(new Document('_id', 2),
                           new Document('$set', new Document('x', 3))),
         new RemoveRequest(new Document('_id', 3)),
         new RemoveRequest(new Document('_id', 4)),
         new ReplaceRequest(new Document('_id', 5),
                            new Document('_id', 5).append('x', 4)),
         new ReplaceRequest(new Document('_id', 6),
                            new Document('_id', 6).append('x', 5)),
         new InsertRequest<Document>(new Document('_id', 7)),
         new InsertRequest<Document>(new Document('_id', 8))
        ]
    }

    private static List<InsertRequest<Document>> getTestInserts() {
        [new InsertRequest<Document>(new Document('_id', 1)),
         new InsertRequest<Document>(new Document('_id', 2)),
         new InsertRequest<Document>(new Document('_id', 3)),
         new InsertRequest<Document>(new Document('_id', 4)),
         new InsertRequest<Document>(new Document('_id', 5)),
         new InsertRequest<Document>(new Document('_id', 6))
        ]
    }
}