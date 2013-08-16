/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.junit.Test
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.WriteConcern
import org.mongodb.codecs.DocumentCodec
import org.mongodb.command.CountOperation

import static org.mongodb.Fixture.getBufferProvider
import static org.mongodb.Fixture.getSession

class InsertOperationSpecification extends FunctionalSpecification {
    def 'should insert a single document'() {
        given:
        def insert = new Insert<Document>(WriteConcern.ACKNOWLEDGED, new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), insert, new DocumentCodec(), getBufferProvider(), getSession(), true);

        when:
        op.execute();

        then:
        insert.getDocuments() == collection.find().into([])
    }

    def 'should insert multiple documents'() {
        given:
        def insert = new Insert<Document>(WriteConcern.ACKNOWLEDGED, [new Document('_id', 1), new Document('_id', 2)])
        def op = new InsertOperation<Document>(getNamespace(), insert, new DocumentCodec(), getBufferProvider(), getSession(), true);

        when:
        op.execute();

        then:
        insert.getDocuments() == collection.find().sort(new Document('_id', 1)).into([])
    }

    def 'should return null CommandResult with unacknowledged WriteConcern'() {
        given:
        def insert = new Insert<Document>(WriteConcern.UNACKNOWLEDGED, new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), insert, new DocumentCodec(), getBufferProvider(), getSession(), true);

        when:
        def result = op.execute();

        then:
        result == null
    }

    @Test
    def 'should insert a batch at The limit of the batch size'() {
        given:

        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 2127];
        byte[] smallerByteArray = new byte[1024 * 16 + 1980];

        List<Document> documents = [
                new Document('bytes', hugeByteArray),
                new Document('bytes', smallerByteArray)
        ]

        Insert<Document> insert = new Insert<Document>(WriteConcern.ACKNOWLEDGED, documents);

        when:
        new InsertOperation<Document>(collection.getNamespace(), insert, new DocumentCodec(), getBufferProvider(), getSession(),
                false).execute();

        then:
        documents.size() ==
                new CountOperation(new Find(), collection.getNamespace(), new DocumentCodec(), getBufferProvider(), getSession(), false)
                        .execute()
    }


}
