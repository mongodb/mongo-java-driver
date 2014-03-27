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



package org.mongodb.operation

import org.junit.Test
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoDuplicateKeyException
import org.mongodb.codecs.DocumentCodec

import static java.util.Arrays.asList
import static org.mongodb.Fixture.getSession
import static org.mongodb.WriteConcern.ACKNOWLEDGED
import static org.mongodb.WriteConcern.UNACKNOWLEDGED

class InsertOperationSpecification extends FunctionalSpecification {
    def 'should return correct result'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, asList(insert), new DocumentCodec());

        when:
        def result = op.execute(getSession());

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
    }

    def 'should insert a single document'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, asList(insert), new DocumentCodec());

        when:
        op.execute(getSession());

        then:
        asList(insert.getDocument()) == collection.find().into([])
    }

    @Test
    public void 'should insert a large number of documents'() {
        given:
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 1001; i++) {
            documents.add(new Document());
        }

        when:
        collection.insert(documents);

        then:
        collection.find().count() == 1001
    }


    def 'should insert multiple documents'() {
        given:
        List<Document> documents = [
                new Document('_id', 1),
                new Document('_id', 2)
        ]

        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, documentsToInserts(documents), new DocumentCodec());

        when:
        op.execute(getSession());

        then:
        documents == collection.find().sort(new Document('_id', 1)).into([])
    }

    def 'should return null CommandResult with unacknowledged WriteConcern'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, UNACKNOWLEDGED, asList(insert), new DocumentCodec());

        when:
        def result = op.execute(getSession());

        then:
        !result.wasAcknowledged()
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

        when:
        new InsertOperation<Document>(collection.getNamespace(), true, ACKNOWLEDGED, documentsToInserts(documents), new DocumentCodec())
                .execute(getSession());

        then:
        documents.size() ==
        new CountOperation(collection.getNamespace(), new Find(), new DocumentCodec())
                .execute(getSession())
    }

    @Test
    def 'should continue on error when continuing on error'() {
        given:

        List<Document> documents = [
                new Document('_id', 1),
                new Document('_id', 1),
                new Document('_id', 2),
        ]

        when:
        new InsertOperation<Document>(collection.getNamespace(), false, ACKNOWLEDGED,
                                      documentsToInserts(documents),
                                      new DocumentCodec())
                .execute(getSession())

        then:
        thrown(MongoDuplicateKeyException)
        2 == new CountOperation(collection.getNamespace(), new Find(), new DocumentCodec())
                .execute(getSession())
    }

    @Test
    def 'should not continue on error when not continuing on error'() {
        given:

        List<Document> documents = [
                new Document('_id', 1),
                new Document('_id', 1),
                new Document('_id', 2),
        ]

        when:
        new InsertOperation<Document>(collection.getNamespace(), true, ACKNOWLEDGED, documentsToInserts(documents), new DocumentCodec())
                .execute(getSession())

        then:
        thrown(MongoDuplicateKeyException)
        1 == new CountOperation(collection.getNamespace(), new Find(), new DocumentCodec())
                .execute(getSession())
    }

    def documentsToInserts(List<Document> documents) {
        def insertList = new ArrayList<InsertRequest<Document>>(documents.size());
        for (def cur : documents) {
            insertList.add(new InsertRequest<Document>(cur));
        }
        insertList
    }
}
