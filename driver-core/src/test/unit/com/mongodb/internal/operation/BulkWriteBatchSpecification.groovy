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

import com.mongodb.MongoBulkWriteException
import com.mongodb.MongoNamespace
import com.mongodb.ReadConcern
import com.mongodb.ServerAddress
import com.mongodb.WriteConcern
import com.mongodb.bulk.BulkWriteInsert
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.BulkWriteUpsert
import com.mongodb.client.model.Collation
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ConnectionDescription
import com.mongodb.connection.ConnectionId
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerId
import com.mongodb.connection.ServerType
import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.bulk.UpdateRequest
import com.mongodb.internal.bulk.WriteRequest
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext
import org.bson.BsonDocument
import org.bson.BsonInt32
import spock.lang.Specification

import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE

class BulkWriteBatchSpecification extends Specification {
    def namespace = new MongoNamespace('db.coll')
    def serverDescription = ServerDescription.builder().address(new ServerAddress()).state(CONNECTED)
            .logicalSessionTimeoutMinutes(30)
            .build()
    def connectionDescription = new ConnectionDescription(
            new ConnectionId(new ServerId(new ClusterId(), serverDescription.getAddress())), 6,
            ServerType.REPLICA_SET_PRIMARY, 1000, 16000, 48000, [])
    def sessionContext = new ReadConcernAwareNoOpSessionContext(ReadConcern.DEFAULT)

    def 'should split payloads by type when ordered'() {
        when:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, true,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests(), sessionContext, null, null)
        def payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'documents'
        payload.getPayload() == getWriteRequestsAsDocuments()[0..0]
        bulkWriteBatch.getCommand() == toBsonDocument('{ "insert" : "coll", "ordered" : true }')
        bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'updates'
        payload.getPayload() == getWriteRequestsAsDocuments()[1..2]
        bulkWriteBatch.getCommand() == toBsonDocument('{ "update" : "coll", "ordered" : true }')
        bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'documents'
        payload.getPayload() == getWriteRequestsAsDocuments()[3..4]
        bulkWriteBatch.getCommand() == toBsonDocument('{ "insert" : "coll", "ordered" : true }')
        bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'updates'
        payload.getPayload() == getWriteRequestsAsDocuments()[5..5]
        bulkWriteBatch.getCommand() == toBsonDocument('{ "update" : "coll", "ordered" : true }')
        bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'deletes'
        payload.getPayload() == getWriteRequestsAsDocuments()[6..7]
        bulkWriteBatch.getCommand() == toBsonDocument('{ "delete" : "coll", "ordered" : true }')
        bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'documents'
        payload.getPayload() == getWriteRequestsAsDocuments()[8..8]
        bulkWriteBatch.getCommand() == toBsonDocument('{ "insert" : "coll", "ordered" : true }')
        bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'deletes'
        payload.getPayload() == getWriteRequestsAsDocuments()[9..9]
        bulkWriteBatch.getCommand() == toBsonDocument('{ "delete" : "coll", "ordered" : true }')
        !bulkWriteBatch.hasAnotherBatch()
    }

    def 'should group payloads by type when unordered'() {
        when:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.MAJORITY, true, false, getWriteRequests(), sessionContext, null, null)
        def payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'documents'
        payload.getPayload() == [getWriteRequestsAsDocuments()[0], getWriteRequestsAsDocuments()[3], getWriteRequestsAsDocuments()[4],
                                 getWriteRequestsAsDocuments()[8]]
        bulkWriteBatch.hasAnotherBatch()
        bulkWriteBatch.getCommand() == toBsonDocument('''{"insert": "coll", "ordered": false,
                "writeConcern": {"w" : "majority"}, "bypassDocumentValidation" : true }''')

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'updates'
        payload.getPayload() == [getWriteRequestsAsDocuments()[1], getWriteRequestsAsDocuments()[2]]
        bulkWriteBatch.hasAnotherBatch()
        bulkWriteBatch.getCommand() == toBsonDocument('''{"update": "coll", "ordered": false,
                "writeConcern": {"w" : "majority"}, "bypassDocumentValidation" : true }''')

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'updates'
        payload.getPayload() == [getWriteRequestsAsDocuments()[5]]
        bulkWriteBatch.hasAnotherBatch()
        bulkWriteBatch.getCommand() == toBsonDocument('''{"update": "coll", "ordered": false,
                "writeConcern": {"w" : "majority"}, "bypassDocumentValidation" : true }''')

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(payload.size())

        then:
        payload.getPayloadName() == 'deletes'
        payload.getPayload() == [getWriteRequestsAsDocuments()[6], getWriteRequestsAsDocuments()[7], getWriteRequestsAsDocuments()[9]]
        !bulkWriteBatch.hasAnotherBatch()
        bulkWriteBatch.getCommand() == toBsonDocument('''{"delete": "coll", "ordered": false,
                "writeConcern": {"w" : "majority"}, "bypassDocumentValidation" : true }''')
    }

    def 'should split payloads if only payload partially processed'() {
        when:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests()[0..3], sessionContext, null, null)
        def payload = bulkWriteBatch.getPayload()
        payload.setPosition(1)

        then:
        payload.getPayloadName() == 'documents'
        payload.getPayload() == [getWriteRequestsAsDocuments()[0], getWriteRequestsAsDocuments()[3]]
        bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(1)

        then:
        payload.getPayloadName() == 'documents'
        payload.getPayload() == [getWriteRequestsAsDocuments()[3]]
        bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(1)

        then:
        payload.getPayloadName() == 'updates'
        payload.getPayload() == [getWriteRequestsAsDocuments()[1], getWriteRequestsAsDocuments()[2]]
        !bulkWriteBatch.hasAnotherBatch()

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(1)

        then:
        payload.getPayloadName() == 'updates'
        payload.getPayload() == [getWriteRequestsAsDocuments()[2]]
        !bulkWriteBatch.hasAnotherBatch()
    }

    def 'should map all inserted ids'() {
        when:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.ACKNOWLEDGED, null, false,
                [new InsertRequest(toBsonDocument('{_id: 0}')),
                 new InsertRequest(toBsonDocument('{_id: 1}')),
                 new InsertRequest(toBsonDocument('{_id: 2}'))
                ],
                sessionContext, null, null)
        def payload = bulkWriteBatch.getPayload()
        payload.setPosition(1)
        payload.insertedIds.put(0, new BsonInt32(0))
        bulkWriteBatch.addResult(BsonDocument.parse('{"n": 1, "ok": 1.0}'))

        then:
        bulkWriteBatch.getResult().inserts == [new BulkWriteInsert(0, new BsonInt32(0))]

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(1)
        payload.insertedIds.put(1, new BsonInt32(1))
        bulkWriteBatch.addResult(BsonDocument.parse('{"n": 1, "ok": 1.0}'))

        then:
        bulkWriteBatch.getResult().inserts == [new BulkWriteInsert(0, new BsonInt32(0)),
                                               new BulkWriteInsert(1, new BsonInt32(1))]

        when:
        bulkWriteBatch = bulkWriteBatch.getNextBatch()
        payload = bulkWriteBatch.getPayload()
        payload.setPosition(1)
        payload.insertedIds.put(2, new BsonInt32(2))
        bulkWriteBatch.addResult(BsonDocument.parse('{"n": 1, "ok": 1.0}'))

        then:
        bulkWriteBatch.getResult().inserts == [new BulkWriteInsert(0, new BsonInt32(0)),
                                               new BulkWriteInsert(1, new BsonInt32(1)),
                                               new BulkWriteInsert(2, new BsonInt32(2))]
    }

    def 'should not map inserted id with a write error'() {
        given:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.ACKNOWLEDGED, null, false,
                [new InsertRequest(toBsonDocument('{_id: 0}')),
                 new InsertRequest(toBsonDocument('{_id: 1}')),
                 new InsertRequest(toBsonDocument('{_id: 2}'))
                ],
                sessionContext, null, null)
        def payload = bulkWriteBatch.getPayload()
        payload.setPosition(3)
        payload.insertedIds.put(0, new BsonInt32(0))
        payload.insertedIds.put(1, new BsonInt32(1))
        payload.insertedIds.put(2, new BsonInt32(2))

        when:
        bulkWriteBatch.addResult(toBsonDocument('''{"ok": 1, "n": 2,
            "writeErrors": [{ "index" : 1, "code" : 11000, "errmsg": "duplicate key error"}] }'''))
        bulkWriteBatch.getResult()

        then:
        def ex = thrown(MongoBulkWriteException)
        ex.getWriteResult().inserts == [new BulkWriteInsert(0, new BsonInt32(0)),
                                        new BulkWriteInsert(2, new BsonInt32(2))]
    }

    def 'should not retry when at least one write is not retryable'() {
        when:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, false,
                WriteConcern.ACKNOWLEDGED, null, true,
                [new DeleteRequest(new BsonDocument()).multi(true), new InsertRequest(new BsonDocument())], sessionContext, null, null)

        then:
        !bulkWriteBatch.getRetryWrites()
    }

    def 'should handle operation responses'() {
        given:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, true,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests()[1..1], sessionContext, null, null)
        def writeConcernError = toBsonDocument('{ok: 1, n: 1, upserted: [{_id: 2, index: 0}]}')

        when:
        bulkWriteBatch.addResult(writeConcernError)

        then:
        !bulkWriteBatch.hasErrors()
        bulkWriteBatch.getResult() == BulkWriteResult.acknowledged(0, 0, 0, 0, [new BulkWriteUpsert(0, new BsonInt32(2))], [])
        bulkWriteBatch.shouldProcessBatch()
    }

    def 'should handle writeConcernError error responses'() {
        given:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, true,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests()[0..0], sessionContext, null, null)
        def writeConcernError = toBsonDocument('{n: 1, writeConcernError: {code: 75, errmsg: "wtimeout", errInfo: {wtimeout: "0"}}}')

        when:
        bulkWriteBatch.addResult(writeConcernError)

        then:
        bulkWriteBatch.hasErrors()
        bulkWriteBatch.getError().getWriteErrors().isEmpty()
        bulkWriteBatch.getError().getWriteConcernError()
        bulkWriteBatch.shouldProcessBatch()
    }

    def 'should handle writeErrors error responses'() {
        given:
        def bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionDescription, true,
                WriteConcern.ACKNOWLEDGED, null, false, getWriteRequests()[0..0], sessionContext, null, null)
        def writeError = toBsonDocument('''{"ok": 0, "n": 1, "code": 65, "errmsg": "bulk op errors",
            "writeErrors": [{ "index" : 0, "code" : 100, "errmsg": "some error"}] }''')

        when:
        bulkWriteBatch.addResult(writeError)

        then:
        bulkWriteBatch.hasErrors()
        bulkWriteBatch.getError().getWriteErrors().size() == 1
        !bulkWriteBatch.shouldProcessBatch()
    }

    private static List<WriteRequest> getWriteRequests() {
        [new InsertRequest(toBsonDocument('{_id: 1, x: 1}')),
         new UpdateRequest(toBsonDocument('{ _id: 2}'), toBsonDocument('{$set: {x : 2}}'), UPDATE).upsert(true),
         new UpdateRequest(toBsonDocument('{ _id: 3}'), toBsonDocument('{$set: {x : 3}}'), UPDATE),
         new InsertRequest(toBsonDocument('{_id: 4, x: 4}')),
         new InsertRequest(toBsonDocument('{_id: 5, x: 5}')),
         new UpdateRequest(toBsonDocument('{ _id: 6}'), toBsonDocument('{_id: 6, x: 6}'), REPLACE)
                 .collation(Collation.builder().locale('en').build()),
         new DeleteRequest(toBsonDocument('{_id: 7}')),
         new DeleteRequest(toBsonDocument('{_id: 8}')),
         new InsertRequest(toBsonDocument('{_id: 9, x: 9}')),
         new DeleteRequest(toBsonDocument('{_id: 10}')).collation(Collation.builder().locale('de').build())
        ]
    }

    private static List<BsonDocument> getWriteRequestsAsDocuments() {
        ['{_id: 1, x: 1}',
         '{"q": { "_id" : 2}, "u": { "$set": {"x": 2}}, "multi": true, "upsert": true }',
         '{"q": { "_id" : 3}, "u": { "$set": {"x": 3}}, "multi": true}',
         '{"_id": 4, "x": 4}',
         '{"_id": 5, "x": 5}',
         '{"q": { "_id" : 6 }, "u": { "_id": 6, "x": 6 }, "collation": { "locale": "en" }}',
         '{"q": { "_id" : 7 }, "limit": 0 }',
         '{"q": { "_id" : 8 }, "limit": 0 }',
         '{"_id": 9, "x": 9}',
         '{"q": { "_id" : 10 }, "limit": 0, "collation" : { "locale" : "de" }}'
        ].collect { toBsonDocument(it) }
    }

    private static BsonDocument toBsonDocument(final String json) {
        BsonDocument.parse(json)
    }
}
