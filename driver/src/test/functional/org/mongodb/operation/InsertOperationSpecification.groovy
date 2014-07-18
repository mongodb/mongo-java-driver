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

import category.Async
import com.mongodb.MongoException
import com.mongodb.codecs.DocumentCodec
import org.bson.BsonSerializationException
import org.bson.types.Binary
import org.junit.experimental.categories.Category
import org.mongodb.Document
import org.mongodb.FunctionalSpecification

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static java.util.Arrays.asList
import static org.mongodb.Fixture.getAsyncBinding
import static org.mongodb.Fixture.getAsyncSingleConnectionBinding
import static org.mongodb.Fixture.getBinding
import static org.mongodb.Fixture.getPinnedBinding

class InsertOperationSpecification extends FunctionalSpecification {
    def 'should return correct result'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, asList(insert), new DocumentCodec())

        when:
        def result = op.execute(getBinding())

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
    }

    @Category(Async)
    def 'should return correct result asynchronously'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, asList(insert), new DocumentCodec())

        when:
        def result = op.executeAsync(getAsyncBinding()).get()

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
    }

    def 'should insert a single document'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, asList(insert), new DocumentCodec())

        when:
        op.execute(getBinding())

        then:
        asList(insert.getDocument()) == getCollectionHelper().find()
    }

    @Category(Async)
    def 'should insert a single document asynchronously'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, asList(insert), new DocumentCodec())

        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        asList(insert.getDocument()) == getCollectionHelper().find()
    }

    def 'should insert a large number of documents'() {
        given:
        def inserts = []
        for (i in 1..1001) {
            inserts += new InsertRequest<Document>(new Document('_id', i))
        }
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, inserts.toList(), new DocumentCodec())


        when:
        op.execute(getBinding())

        then:
        getCollectionHelper().count() == 1001
    }

    @Category(Async)
    def 'should insert a large number of documents asynchronously'() {
        given:
        def inserts = []
        for (i in 1..1001) {
            inserts += new InsertRequest<Document>(new Document('_id', i))
        }
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, inserts.toList(), new DocumentCodec())


        when:
        op.executeAsync(getAsyncBinding()).get()

        then:
        getCollectionHelper().count() == 1001
    }

    def 'should return UnacknowledgedWriteResult when using an unacknowledged WriteConcern'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, UNACKNOWLEDGED, asList(insert), new DocumentCodec())

        when:
        def binding = getPinnedBinding()
        def result = op.execute(binding)
        new InsertOperation<Document>(namespace, true, ACKNOWLEDGED, [new InsertRequest<Document>(new Document('_id', 2))],
                                      new DocumentCodec()).execute(binding);

        then:
        !result.wasAcknowledged()

        cleanup:
        acknowledgeWrite(binding)
    }

    @Category(Async)
    def 'should return UnacknowledgedWriteResult when using an unacknowledged WriteConcern asynchronously'() {
        given:
        def insert = new InsertRequest<Document>(new Document('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, UNACKNOWLEDGED, asList(insert), new DocumentCodec())

        when:
        def binding = getAsyncSingleConnectionBinding()
        def result = op.executeAsync(binding).get()

        then:
        !result.wasAcknowledged()

        cleanup:
        acknowledgeWrite(binding)
    }

    def 'should insert a batch at The limit of the batch size'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 2127];
        byte[] smallerByteArray = new byte[1024 * 16 + 1980];

        def documents = [
                new InsertRequest<Document>(new Document('bytes', new Binary(hugeByteArray))),
                new InsertRequest<Document>(new Document('bytes', new Binary(smallerByteArray)))
        ]

        when:
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, documents.toList(), new DocumentCodec()).execute(getBinding())

        then:
        getCollectionHelper().count() == 2
    }

    @Category(Async)
    def 'should insert a batch at The limit of the batch size asynchronously'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16 - 2127];
        byte[] smallerByteArray = new byte[1024 * 16 + 1980];

        def documents = [
                new InsertRequest<Document>(new Document('bytes', new Binary(hugeByteArray))),
                new InsertRequest<Document>(new Document('bytes', new Binary(smallerByteArray)))
        ]

        when:
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, documents.toList(), new DocumentCodec())
                .executeAsync(getAsyncBinding()).get()

        then:
        getCollectionHelper().count() == 2
    }

    def 'should continue on error when continuing on error'() {
        given:
        def documents = [
                new InsertRequest<Document>(new Document('_id', 1)),
                new InsertRequest<Document>(new Document('_id', 1)),
                new InsertRequest<Document>(new Document('_id', 2)),
        ]

        when:
        new InsertOperation<Document>(getNamespace(), false, ACKNOWLEDGED, documents, new DocumentCodec())
                .execute(getBinding())

        then:
        thrown(MongoException.DuplicateKey)
        getCollectionHelper().count() == 2
    }

    @Category(Async)
    def 'should continue on error when continuing on error asynchronously'() {
        given:
        def documents = [
                new InsertRequest<Document>(new Document('_id', 1)),
                new InsertRequest<Document>(new Document('_id', 1)),
                new InsertRequest<Document>(new Document('_id', 2)),
        ]

        when:
        new InsertOperation<Document>(getNamespace(), false, ACKNOWLEDGED, documents, new DocumentCodec())
                .executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoException.DuplicateKey)
        getCollectionHelper().count() == 2
    }

    def 'should not continue on error when not continuing on error'() {
        given:
        def documents = [
                new InsertRequest<Document>(new Document('_id', 1)),
                new InsertRequest<Document>(new Document('_id', 1)),
                new InsertRequest<Document>(new Document('_id', 2)),
        ]

        when:
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, documents, new DocumentCodec())
                .execute(getBinding())

        then:
        thrown(MongoException.DuplicateKey)
        getCollectionHelper().count() == 1
    }

    @Category(Async)
    def 'should not continue on error when not continuing on error asynchronously'() {
        given:
        def documents = [
                new InsertRequest<Document>(new Document('_id', 1)),
                new InsertRequest<Document>(new Document('_id', 1)),
                new InsertRequest<Document>(new Document('_id', 2)),
        ]

        when:
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, documents, new DocumentCodec())
                .executeAsync(getAsyncBinding()).get()

        then:
        thrown(MongoException.DuplicateKey)
        getCollectionHelper().count() == 1
    }

    def 'should throw exception if document is too large'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16];
        def documents = [new InsertRequest<Document>(new Document('_id', 1).append('b', new Binary(hugeByteArray)))]

        when:
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, documents, new DocumentCodec())
                .execute(getBinding())

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw exception if document is too large asynchronously'() {
        given:
        byte[] hugeByteArray = new byte[1024 * 1024 * 16];
        def documents = [new InsertRequest<Document>(new Document('_id', 1).append('b', new Binary(hugeByteArray)))]

        when:
        new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, documents, new DocumentCodec())
                .executeAsync(getAsyncBinding()).get()

        then:
        thrown(BsonSerializationException)
    }

    def 'should move _id to the beginning'() {
        given:
        def insert = new InsertRequest<Document>(new Document('x', 1).append('_id', 1))
        def op = new InsertOperation<Document>(getNamespace(), true, ACKNOWLEDGED, asList(insert), new DocumentCodec())

        when:
        op.execute(getBinding())

        then:
        getCollectionHelper().find().get(0).keySet() as List == ['_id', 'x']
    }
}
