/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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











package org.mongodb.protocol

import org.mongodb.BulkWriteException
import org.mongodb.BulkWriteUpsert
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.codecs.DocumentCodec
import org.mongodb.operation.CountOperation
import org.mongodb.operation.Find
import org.mongodb.operation.InsertRequest
import org.mongodb.operation.UpdateRequest
import org.mongodb.session.PrimaryServerSelector

import static java.util.Arrays.asList
import static java.util.concurrent.TimeUnit.SECONDS
import static org.junit.Assume.assumeTrue
import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getCluster
import static org.mongodb.Fixture.getPrimary
import static org.mongodb.Fixture.getSession
import static org.mongodb.Fixture.serverVersionAtLeast
import static org.mongodb.WriteConcern.ACKNOWLEDGED

class WriteCommandProtocolSpecification extends FunctionalSpecification {

    def server = getCluster().selectServer(new PrimaryServerSelector(), 1, SECONDS)
    def connection = server.connection

    def setup() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
    }

    def cleanup() {
        connection?.close()
    }

    def 'should insert a document'() {
        given:
        def document = new Document('_id', 1)

        def insertRequest = [new InsertRequest(document)]
        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, insertRequest, new DocumentCodec(),
                                                 getBufferProvider(), server.description, connection, false)
        when:
        def result = protocol.execute()

        then:
        result.insertedCount == 1
        result.upserts == []
        QueryResult res = new QueryProtocol(getNamespace(), new Find(document), new DocumentCodec(), new DocumentCodec(),
                                            getBufferProvider(), server.description, connection, false).execute()
        res.results.get(0) == document
    }

    def 'should insert documents'() {
        def requests = [new InsertRequest(new Document('_id', 1)), new InsertRequest(new Document('_id', 2))]
        given:
        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, requests,
                                                 new DocumentCodec(), getBufferProvider(), server.description, connection, false)
        when:
        protocol.execute()

        then:
        QueryResult res = new QueryProtocol(getNamespace(), new Find(), new DocumentCodec(), new DocumentCodec(),
                                            getBufferProvider(), server.description, connection, false).execute()
        res.results.size() == 2
    }

    def 'should throw exception'() {
        given:
        def protocol = new InsertCommandProtocol(getNamespace(), false, ACKNOWLEDGED,
                                                 [new InsertRequest(new Document('_id', 1)), new InsertRequest(new Document('_id', 2))],
                                                 new DocumentCodec(), getBufferProvider(), server.description, connection, false)
        protocol.execute()

        when:
        protocol.execute()  // now do it again

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
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 100];

        def documents = [
                new Document('_id', 1).append('bytes', hugeByteArray),
                new Document('_id', 2).append('bytes', hugeByteArray),
                new Document('_id', 3).append('bytes', hugeByteArray),
                new Document('_id', 4).append('bytes', hugeByteArray)
        ]

        List<InsertRequest<Document>> insertList = new ArrayList<InsertRequest<Document>>(documents.size());
        for (Document cur : documents) {
            insertList.add(new InsertRequest<Document>(cur));
        }
        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, insertList, new DocumentCodec(),
                                                 getBufferProvider(), server.description, connection, false)

        when:
        def result = protocol.execute()

        then:
        result.insertedCount == 4
        documents.size() == new CountOperation(collection.getNamespace(), new Find(), new DocumentCodec(),
                                               getBufferProvider(), getSession(), false).execute()
    }

    def 'should have correct list of processed and unprocessed requests after error on split'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 100];

        def documents = [
                new Document('_id', 1).append('bytes', hugeByteArray),
                new Document('_id', 2).append('bytes', hugeByteArray),
                new Document('_id', 3).append('bytes', hugeByteArray),
                new Document('_id', 4).append('bytes', hugeByteArray)
        ]

        List<InsertRequest<Document>> insertList = new ArrayList<InsertRequest<Document>>(documents.size());
        for (Document cur : documents) {
            insertList.add(new InsertRequest<Document>(cur));
        }

        // Force a duplicate key error in the second insert request
        new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, [new InsertRequest(new Document('_id', 2))],
                                  new DocumentCodec(), getBufferProvider(), server.description, connection, false).execute()

        def protocol = new InsertCommandProtocol(getNamespace(), true, ACKNOWLEDGED, insertList, new DocumentCodec(),
                                                 getBufferProvider(), server.description, connection, false)

        when:
        protocol.execute()

        then:
        def e = thrown(BulkWriteException)

        e.writeResult.insertedCount == 1
    }

    def 'should map indices in exception when split is required'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 100];

        def documents = [
                new Document('_id', 1),
                new Document('_id', 2),
                new Document('_id', 3),
                new Document('_id', 4)
        ]

        List<InsertRequest<Document>> insertList = new ArrayList<InsertRequest<Document>>(documents.size());
        for (Document cur : documents) {
            insertList.add(new InsertRequest<Document>(cur));
        }
        new InsertCommandProtocol(getNamespace(), false, ACKNOWLEDGED, insertList,
                                  new DocumentCodec(), getBufferProvider(), server.description, connection, false).execute()

        // add a large byte array to each document to force a split after each
        for (def document : documents) {
            document.append('bytes', hugeByteArray);
        }
        documents[1].put('_id', 5)  // Make the second document a new one

        def protocol = new InsertCommandProtocol(getNamespace(), false, ACKNOWLEDGED, insertList,
                                                 new DocumentCodec(), getBufferProvider(), server.description, connection, false)
        when:
        protocol.execute()

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
                                                 [new UpdateRequest(new Document('_id', 1), new Document('$set', new Document('x', 1)))
                                                          .upsert(true),
                                                  new UpdateRequest(new Document('_id', 2), new Document('$set', new Document('x', 2)))
                                                          .upsert(true)],
                                                 new DocumentCodec(), getBufferProvider(), server.description, connection, false);

        when:
        def result = protocol.execute();

        then:
        result.updatedCount == 0;
        result.upserts == [new BulkWriteUpsert(0, 1), new BulkWriteUpsert(1, 2)]
    }
}