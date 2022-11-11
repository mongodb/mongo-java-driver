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

package com.mongodb

import com.mongodb.internal.bulk.DeleteRequest
import com.mongodb.internal.bulk.InsertRequest
import com.mongodb.internal.bulk.UpdateRequest
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonObjectId
import org.bson.Document
import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DocumentCodec
import org.bson.types.ObjectId
import spock.lang.IgnoreIf

import static com.mongodb.ClusterFixture.getBinding
import static com.mongodb.ClusterFixture.getSingleConnectionBinding
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet
import static com.mongodb.ClusterFixture.serverVersionLessThan
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForDelete
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForInsert
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForReplace
import static com.mongodb.LegacyMixedBulkWriteOperation.createBulkWriteOperationForUpdate
import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE
import static java.util.Arrays.asList

class LegacyMixedBulkWriteOperationSpecification extends OperationFunctionalSpecification {

    def 'should throw IllegalArgumentException for empty list of requests'() {
        when:
        createBulkWriteOperationForInsert(getNamespace(), true, ACKNOWLEDGED, true, [])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should return correct result for insert'() {
        given:
        def inserts = [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                       new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))]
        def operation = createBulkWriteOperationForInsert(getNamespace(), true, ACKNOWLEDGED, false, inserts)

        when:
        def result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()

        inserts*.getDocument() == getCollectionHelper().find(new BsonDocumentCodec())
    }

    def 'should insert a single document'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = createBulkWriteOperationForInsert(getNamespace(), true, ACKNOWLEDGED, false, asList(insert))

        when:
        execute(operation)

        then:
        asList(insert.getDocument()) == getCollectionHelper().find(new BsonDocumentCodec())
    }

    def 'should execute unacknowledged write'() {
        given:
        def binding = getSingleConnectionBinding()
        def operation = createBulkWriteOperationForInsert(getNamespace(), true, UNACKNOWLEDGED, false,
                [new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                 new InsertRequest(new BsonDocument('_id', new BsonInt32(2)))])

        when:
        def result = execute(operation, binding)

        then:
        !result.wasAcknowledged()
        getCollectionHelper().count(binding) == 2

        cleanup:
        binding?.release()
    }

    def 'should continue on error when continuing on error'() {
        given:
        def documents = [
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(2))),
        ]
        def operation = createBulkWriteOperationForInsert(getNamespace(), false, ACKNOWLEDGED, false, documents)

        when:
        execute(operation)

        then:
        thrown(DuplicateKeyException)
        getCollectionHelper().count() == 2
    }

    def 'should not continue on error when not continuing on error'() {
        given:
        def documents = [
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(1))),
                new InsertRequest(new BsonDocument('_id', new BsonInt32(2))),
        ]
        def operation = createBulkWriteOperationForInsert(getNamespace(), true, ACKNOWLEDGED, false, documents)

        when:
        execute(operation)

        then:
        thrown(DuplicateKeyException)
        getCollectionHelper().count() == 1
    }

    @IgnoreIf({ serverVersionLessThan(3, 6) || !isDiscoverableReplicaSet() })
    def 'should support retryable writes'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        def operation = createBulkWriteOperationForInsert(getNamespace(), true, ACKNOWLEDGED, true, asList(insert))

        when:
        executeWithSession(operation, false)

        then:
        asList(insert.getDocument()) == getCollectionHelper().find(new BsonDocumentCodec())
    }

    def 'should remove a document'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document('_id', 1))
        def operation = createBulkWriteOperationForDelete(getNamespace(), true, ACKNOWLEDGED, false,
                [new DeleteRequest(new BsonDocument('_id', new BsonInt32(1)))])

        when:
        def result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == null
        !result.isUpdateOfExisting()
        getCollectionHelper().count() == 0
    }

    def 'should return correct result for replace'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument(), new BsonDocument('_id', new BsonInt32(1)), REPLACE)
        def operation = createBulkWriteOperationForReplace(getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        def result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
    }

    def 'should replace a single document'() {
        given:
        def insert = new InsertRequest(new BsonDocument('_id', new BsonInt32(1)))
        createBulkWriteOperationForInsert(getNamespace(), true, ACKNOWLEDGED, false, asList(insert)).execute(getBinding())

        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
        def operation = createBulkWriteOperationForReplace(getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        def result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == null
        result.isUpdateOfExisting()
        asList(replacement.getUpdateValue()) == getCollectionHelper().find(new BsonDocumentCodec())
        getCollectionHelper().find().get(0).keySet().iterator().next() == '_id'
    }

    def 'should upsert a single document'() {
        given:
        def replacement = new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                new BsonDocument('_id', new BsonInt32(1)).append('x', new BsonInt32(1)), REPLACE)
                .upsert(true)
        def operation = createBulkWriteOperationForReplace(getNamespace(), true, ACKNOWLEDGED, false, asList(replacement))

        when:
        execute(operation)

        then:
        asList(replacement.getUpdateValue()) == getCollectionHelper().find(new BsonDocumentCodec())
    }

    def 'should update nothing if no documents match'() {
        given:
        def operation = createBulkWriteOperationForUpdate(getNamespace(), true, ACKNOWLEDGED, false,
                asList(new UpdateRequest(new BsonDocument('x', new BsonInt32(1)),
                        new BsonDocument('$set', new BsonDocument('y', new BsonInt32(2))), UPDATE).multi(false)))

        when:
        WriteConcernResult result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 0
        result.upsertedId == null
        !result.isUpdateOfExisting()
        getCollectionHelper().count() == 0
    }

    def 'when multi is false should update one matching document'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document('x', 1),
                new Document('x', 1))
        def operation = createBulkWriteOperationForUpdate(getNamespace(), true, ACKNOWLEDGED, false,
                asList(new UpdateRequest(new BsonDocument('x', new BsonInt32(1)),
                        new BsonDocument('$set', new BsonDocument('y', new BsonInt32(2))), UPDATE).multi(false)))

        when:
        WriteConcernResult result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == null
        result.isUpdateOfExisting()
        getCollectionHelper().count(new Document('y', 2)) == 1
    }

    def 'when multi is true should update all matching documents'() {
        given:
        getCollectionHelper().insertDocuments(new DocumentCodec(),
                new Document('x', 1),
                new Document('x', 1))
        def operation = createBulkWriteOperationForUpdate(getNamespace(), true, ACKNOWLEDGED, false,
                asList(new UpdateRequest(new BsonDocument('x', new BsonInt32(1)),
                        new BsonDocument('$set', new BsonDocument('y', new BsonInt32(2))), UPDATE).multi(true)))

        when:
        WriteConcernResult result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 2
        result.upsertedId == null
        result.isUpdateOfExisting()
        getCollectionHelper().count(new Document('y', 2)) == 2
    }

    def 'when upsert is true should insert a document if there are no matching documents'() {
        given:
        def operation = createBulkWriteOperationForUpdate(getNamespace(), true, ACKNOWLEDGED, false,
                asList(new UpdateRequest(new BsonDocument('_id', new BsonInt32(1)),
                        new BsonDocument('$set', new BsonDocument('y', new BsonInt32(2))), UPDATE).upsert(true)))

        when:
        WriteConcernResult result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == new BsonInt32(1)
        !result.isUpdateOfExisting()
        getCollectionHelper().count(new Document('y', 2)) == 1
    }

    def 'should return correct result for upsert'() {
        given:
        def id = new ObjectId()
        def operation = createBulkWriteOperationForUpdate(getNamespace(), true, ACKNOWLEDGED, false,
                asList(new UpdateRequest(new BsonDocument('_id', new BsonObjectId(id)),
                        new BsonDocument('$set', new BsonDocument('x', new BsonInt32(1))), UPDATE).upsert(true)))

        when:
        WriteConcernResult result = execute(operation)

        then:
        result.wasAcknowledged()
        result.count == 1
        result.upsertedId == new BsonObjectId(id)
        !result.isUpdateOfExisting()
    }
}
