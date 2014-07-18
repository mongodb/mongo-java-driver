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


package com.mongodb.protocol

import com.mongodb.codecs.DocumentCodec
import com.mongodb.selector.PrimaryServerSelector
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.types.Binary
import org.mongodb.BulkWriteException
import org.mongodb.BulkWriteUpsert
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.operation.CountOperation
import org.mongodb.operation.Find
import org.mongodb.operation.InsertRequest
import org.mongodb.operation.UpdateRequest

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.getBinding
import static org.mongodb.Fixture.getCluster
import static org.mongodb.Fixture.getPrimary
import static org.mongodb.Fixture.serverVersionAtLeast

class WriteCommandProtocolSpecification extends FunctionalSpecification {

    def server = getCluster().selectServer(new PrimaryServerSelector(), 1, SECONDS)
    def connection = server.connection

    def setup() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
    }

    def cleanup() {
        connection?.release()
    }

    def 'should insert a document'() {
        given:
        def document = new Document('_id', 1)

        def insertRequest = [new InsertRequest(document)]
        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, insertRequest, new DocumentCodec())
        when:
        def result = protocol.execute(connection)

        then:
        result.insertedCount == 1
        result.upserts == []
        collection.find(document).one == document
    }

    def 'should insert documents'() {
        def requests = [new InsertRequest(new Document('_id', 1)), new InsertRequest(new Document('_id', 2))]
        given:
        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, requests,
                                                 new DocumentCodec())
        when:
        protocol.execute(connection)

        then:
        collection.find().count() == 2
    }

    def 'should throw exception'() {
        given:
        def protocol = new InsertCommandProtocol(getNamespace(), false, ACKNOWLEDGED,
                                                 [new InsertRequest(new Document('_id', 1)), new InsertRequest(new Document('_id', 2))],
                                                 new DocumentCodec())
        protocol.execute(connection)

        when:
        protocol.execute(connection)  // now do it again

        then:
        def e = thrown(BulkWriteException)
        e.serverAddress == getPrimary()
        e.writeErrors.size() == 2
        e.writeResult != null
        e.writeResult.insertedCount == 0;
        e.writeResult.upserts == []
        with(e.writeErrors[0]) {
            index == 0
            code == 11000
            message != null
        }
        with(e.writeErrors[1]) {
            index == 1
            code == 11000
            message != null
        }
    }

    def 'should split a large batch'() {
        given:
        def hugeBinary = new Binary(new byte[1024 * 1024 * 16 - 100]);

        def documents = [
                new Document('_id', 1).append('bytes', hugeBinary),
                new Document('_id', 2).append('bytes', hugeBinary),
                new Document('_id', 3).append('bytes', hugeBinary),
                new Document('_id', 4).append('bytes', hugeBinary)
        ]

        List<InsertRequest<Document>> insertList = new ArrayList<InsertRequest<Document>>(documents.size());
        for (
                Document cur :
                        documents) {
            insertList.add(new InsertRequest<Document>(cur));
        }
        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, insertList, new DocumentCodec())

        when:
        def result = protocol.execute(connection)

        then:
        result.insertedCount == 4
        documents.size() == new CountOperation(collection.getNamespace(), new Find()).execute(getBinding())
    }

    def 'should have correct list of processed and unprocessed requests after error on split'() {
        given:
        def hugeBinary = new Binary(new byte[1024 * 1024 * 16 - 100]);

        def documents = [
                new Document('_id', 1).append('bytes', hugeBinary),
                new Document('_id', 2).append('bytes', hugeBinary),
                new Document('_id', 3).append('bytes', hugeBinary),
                new Document('_id', 4).append('bytes', hugeBinary)
        ]

        List<InsertRequest<Document>> insertList = new ArrayList<InsertRequest<Document>>(documents.size());
        for (
                Document cur :
                        documents) {
            insertList.add(new InsertRequest<Document>(cur));
        }

        // Force a duplicate key error in the second insert request
        new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, [new InsertRequest(new Document('_id', 2))],
                                  new DocumentCodec()).execute(connection)

        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, insertList, new DocumentCodec())

        when:
        protocol.execute(connection)

        then:
        def e = thrown(BulkWriteException)

        e.writeResult.insertedCount == 1
    }

    def 'should map indices in exception when split is required'() {
        given:
        def hugeBinary = new Binary(new byte[1024 * 1024 * 16 - 100]);

        def documents = [
                new Document('_id', 1),
                new Document('_id', 2),
                new Document('_id', 3),
                new Document('_id', 4)
        ]

        List<InsertRequest<Document>> insertList = new ArrayList<InsertRequest<Document>>(documents.size());
        for (
                Document cur :
                        documents) {
            insertList.add(new InsertRequest<Document>(cur));
        }
        new InsertCommandProtocol(getNamespace(), false, ACKNOWLEDGED, insertList,
                                  new DocumentCodec()).execute(connection)

        // add a large byte array to each document to force a split after each
        for (
                def document :
                        documents) {
            document.append('bytes', hugeBinary);
        }
        documents[1].put('_id', 5)  // Make the second document a new one

        def protocol = new InsertCommandProtocol(getNamespace(), false, ACKNOWLEDGED, insertList,
                                                 new DocumentCodec())
        when:
        protocol.execute(connection)

        then:
        def e = thrown(BulkWriteException)
        e.serverAddress == getPrimary()
        e.writeErrors.size() == 3
        e.writeErrors[0].index == 0
        e.writeErrors[1].index == 2
        e.writeErrors[2].index == 3
    }

    def 'should upsert items'() {
        given:
        def protocol = new UpdateCommandProtocol(getNamespace(), true, ACKNOWLEDGED,
                                                 [new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                                                                    new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1))))
                                                          .upsert(true),
                                                  new UpdateRequest(new BsonDocument('_id', new BsonInt32(2)),
                                                                    new BsonDocument('$set', new BsonDocument('x', new BsonInt32(2))))
                                                          .upsert(true)],
                                                 new DocumentCodec());

        when:
        def result = protocol.execute(connection);

        then:
        result.matchedCount == 0;
        result.upserts == [new BulkWriteUpsert(0, new BsonInt32(1)), new BulkWriteUpsert(1, new BsonInt32(2))]
    }
}
