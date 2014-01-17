/*
 * Copyright (c) 2008 - 2013 MongoDB Inc. <http://10gen.com>
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

import org.bson.types.ObjectId

import static com.mongodb.WriteRequest.Type.INSERT
import static com.mongodb.WriteRequest.Type.REMOVE
import static com.mongodb.WriteRequest.Type.REPLACE
import static com.mongodb.WriteRequest.Type.UPDATE

class BulkWriteOperationSpecification extends FunctionalSpecification {

    def 'when no document with the same id exists, should insert the document'() {
        given:
        def builder = collection.initializeOrderedBulkOperation();
        builder.insert(new BasicDBObject('_id', 1))

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(INSERT, 1, [])
        result.getUpserts() == []
        collection.findOne() == new BasicDBObject('_id', 1)
    }

    def 'when a document with the same id exists, should throw an exception'() {
        given:
        def document = new BasicDBObject('_id', 1)
        collection.insert(document)
        def builder = collection.initializeOrderedBulkOperation();
        builder.insert(document)

        when:
        builder.execute()

        then:
        def ex = thrown(BulkWriteException)
        ex.getWriteErrors().get(0).code == 11000
    }

    def 'when documents match the query, a remove of one should remove one of them'() {
        given:
        collection.insert(new BasicDBObject('x', true))
        collection.insert(new BasicDBObject('x', true))
        def builder = collection.initializeOrderedBulkOperation()
        builder.find(new BasicDBObject('x', true)).removeOne()

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(REMOVE, 1, [])
        collection.count() == 1
    }

    def 'when documents match the query, a remove should remove all of them'() {
        given:
        collection.insert(new BasicDBObject('x', true))
        collection.insert(new BasicDBObject('x', true))
        collection.insert(new BasicDBObject('x', false))
        def builder = collection.initializeOrderedBulkOperation()
        builder.find(new BasicDBObject('x', true)).remove()

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(REMOVE, 2, [])
        collection.count() == 1
    }

    def 'when a document matches the query, an update of one should update that document'() {
        given:
        collection.insert(new BasicDBObject('x', true))
        collection.insert(new BasicDBObject('x', true))

        def builder = collection.initializeOrderedBulkOperation()
        builder.find(new BasicDBObject('x', true)).updateOne(new BasicDBObject('$set', new BasicDBObject('y', 1)))

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 1, 1, [])
        collection.count(new BasicDBObject('y', 1)) == 1
    }

    def 'when documents match the query, an update should update all of them'() {
        given:
        collection.insert(new BasicDBObject('x', true))
        collection.insert(new BasicDBObject('x', true))
        collection.insert(new BasicDBObject('x', false))

        def builder = collection.initializeOrderedBulkOperation()
        builder.find(new BasicDBObject('x', true)).update(new BasicDBObject('$set', new BasicDBObject('y', 1)))

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 2, 2, [])
        collection.count(new BasicDBObject('y', 1)) == 2
    }

    def 'when no document matches the query, an update with upsert should insert a document'() {
        given:
        def id = new ObjectId()
        def builder = collection.initializeOrderedBulkOperation()
        builder.find(new BasicDBObject('_id', id)).upsert().updateOne(new BasicDBObject('$set', new BasicDBObject('x', 2)))

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 0, [new BulkWriteUpsert(0, id)])
        collection.findOne() == new BasicDBObject('_id', id).append('x', 2)
    }

    def 'when no document matches the query, a replace with upsert should insert a document'() {
        given:
        def id = new ObjectId()
        def builder = collection.initializeOrderedBulkOperation()
        def query = new BasicDBObject('_id', id)
        builder.find(query).upsert().replaceOne(new BasicDBObject('_id', id).append('x', 2))

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 0, [new BulkWriteUpsert(0, id)])
        collection.findOne() == new BasicDBObject('_id', id).append('x', 2)
    }

    def 'when a document matches the query, an update with upsert should update that document'() {
        given:
        def id = new ObjectId()
        collection.insert(new BasicDBObject('_id', id))
        def builder = collection.initializeOrderedBulkOperation()
        builder.find(new BasicDBObject('_id', id)).upsert().updateOne(new BasicDBObject('$set', new BasicDBObject('x', 2)))

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 1, 1, [])
        collection.findOne() == new BasicDBObject('_id', id).append('x', 2)
    }

    def 'when a document matches the query, a replace with upsert should update that document'() {
        given:
        collection.insert(new BasicDBObject('_id', 1))

        def builder = collection.initializeOrderedBulkOperation()
        builder.find(new BasicDBObject('_id', 1)).upsert().replaceOne(new BasicDBObject('_id', 1).append('x', 2))

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(REPLACE, 1, 1, [])
        collection.findOne() == new BasicDBObject('_id', 1).append('x', 2)
    }

    def 'should handle multi-length runs of ordered insert, update, replace, and remove'() {
        given:
        collection.insert(getTestInserts())

        def builder = collection.initializeOrderedBulkOperation()
        addWritesToBuilder(builder)

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(2, 4, 2, 4, [])

        collection.findOne(new BasicDBObject('_id', 1)) == new BasicDBObject('_id', 1).append('x', 2)
        collection.findOne(new BasicDBObject('_id', 2)) == new BasicDBObject('_id', 2).append('x', 3)
        collection.findOne(new BasicDBObject('_id', 3)) == null
        collection.findOne(new BasicDBObject('_id', 4)) == null
        collection.findOne(new BasicDBObject('_id', 5)) == new BasicDBObject('_id', 5).append('x', 4)
        collection.findOne(new BasicDBObject('_id', 6)) == new BasicDBObject('_id', 6).append('x', 5)
        collection.findOne(new BasicDBObject('_id', 7)) == new BasicDBObject('_id', 7)
        collection.findOne(new BasicDBObject('_id', 8)) == new BasicDBObject('_id', 8)
    }

    def 'should handle multi-length runs of unacknowledged insert, update, replace, and remove'() {
        given:
        collection.insert(getTestInserts())

        def builder = collection.initializeOrderedBulkOperation()
        addWritesToBuilder(builder)

        when:
        def result = builder.execute(WriteConcern.UNACKNOWLEDGED)

        then:
        !result.isAcknowledged()
        collection.findOne(new BasicDBObject('_id', 1)) == new BasicDBObject('_id', 1).append('x', 2)
        collection.findOne(new BasicDBObject('_id', 2)) == new BasicDBObject('_id', 2).append('x', 3)
        collection.findOne(new BasicDBObject('_id', 3)) == null
        collection.findOne(new BasicDBObject('_id', 4)) == null
        collection.findOne(new BasicDBObject('_id', 5)) == new BasicDBObject('_id', 5).append('x', 4)
        collection.findOne(new BasicDBObject('_id', 6)) == new BasicDBObject('_id', 6).append('x', 5)
        collection.findOne(new BasicDBObject('_id', 7)) == new BasicDBObject('_id', 7)
        collection.findOne(new BasicDBObject('_id', 8)) == new BasicDBObject('_id', 8)
    }

    def 'error details should have correct index on ordered write failure'() {
        given:
        def builder = collection.initializeOrderedBulkOperation()
        builder.insert(new BasicDBObject('_id', 1))
        builder.find(new BasicDBObject('_id', 1)).updateOne(new BasicDBObject('$set', new BasicDBObject('x', 3)))
        builder.insert(new BasicDBObject('_id', 1))

        when:
        builder.execute()

        then:
        def ex = thrown(BulkWriteException)
        ex.writeErrors.size() == 1
        ex.writeErrors[0].index == 2
        ex.writeErrors[0].code == 11000
    }

    def 'should handle multi-length runs of unordered insert, update, replace, and remove'() {
        given:
        collection.insert(getTestInserts());
        def builder = collection.initializeUnorderedBulkOperation()
        addWritesToBuilder(builder)

        when:
        def result = builder.execute()

        then:
        result == new AcknowledgedBulkWriteResult(2, 4, 2, 4, [])

        collection.findOne(new BasicDBObject('_id', 1)) == new BasicDBObject('_id', 1).append('x', 2)
        collection.findOne(new BasicDBObject('_id', 2)) == new BasicDBObject('_id', 2).append('x', 3)
        collection.findOne(new BasicDBObject('_id', 3)) == null
        collection.findOne(new BasicDBObject('_id', 4)) == null
        collection.findOne(new BasicDBObject('_id', 5)) == new BasicDBObject('_id', 5).append('x', 4)
        collection.findOne(new BasicDBObject('_id', 6)) == new BasicDBObject('_id', 6).append('x', 5)
        collection.findOne(new BasicDBObject('_id', 7)) == new BasicDBObject('_id', 7)
        collection.findOne(new BasicDBObject('_id', 8)) == new BasicDBObject('_id', 8)
    }

    def 'error details should have correct index on unordered write failure'() {
        given:
        collection.insert(getTestInserts())

        def builder = collection.initializeUnorderedBulkOperation()
        builder.insert(new BasicDBObject('_id', 1))
        builder.find(new BasicDBObject('_id', 2)).updateOne(new BasicDBObject('$set', new BasicDBObject('x', 3)))
        builder.insert(new BasicDBObject('_id', 3))

        when:
        builder.execute()

        then:
        def ex = thrown(BulkWriteException)
        ex.writeErrors.size() == 2
        ex.writeErrors[0].index == 0
        ex.writeErrors[0].code == 11000
        ex.writeErrors[1].index == 2
        ex.writeErrors[1].code == 11000
    }

    private static void addWritesToBuilder(BulkWriteOperation builder) {
        builder.find(new BasicDBObject('_id', 1)).updateOne(new BasicDBObject('$set', new BasicDBObject('x', 2)))
        builder.find(new BasicDBObject('_id', 2)).updateOne(new BasicDBObject('$set', new BasicDBObject('x', 3)))
        builder.find(new BasicDBObject('_id', 3)).removeOne()
        builder.find(new BasicDBObject('_id', 4)).removeOne()
        builder.find(new BasicDBObject('_id', 5)).replaceOne(new BasicDBObject('_id', 5).append('x', 4))
        builder.find(new BasicDBObject('_id', 6)).replaceOne(new BasicDBObject('_id', 6).append('x', 5))
        builder.insert(new BasicDBObject('_id', 7))
        builder.insert(new BasicDBObject('_id', 8))
    }

    private static List<BasicDBObject> getTestInserts() {
        [new BasicDBObject('_id', 1),
         new BasicDBObject('_id', 2),
         new BasicDBObject('_id', 3),
         new BasicDBObject('_id', 4),
         new BasicDBObject('_id', 5),
         new BasicDBObject('_id', 6)
        ]
    }
}