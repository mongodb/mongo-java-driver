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

package com.mongodb.connection

import com.mongodb.MongoBulkWriteException
import com.mongodb.ServerAddress
import com.mongodb.bulk.BulkWriteError
import com.mongodb.bulk.BulkWriteResult
import com.mongodb.bulk.BulkWriteUpsert
import com.mongodb.bulk.WriteConcernError
import com.mongodb.internal.connection.IndexMap
import org.bson.BsonDocument
import org.bson.BsonString
import spock.lang.Specification

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.bulk.WriteRequest.Type.INSERT
import static com.mongodb.bulk.WriteRequest.Type.UPDATE

class BulkWriteBatchCombinerSpecification extends Specification {
    def 'should get unacknowledged result for an unacknowledged write'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, UNACKNOWLEDGED)
        combiner.addResult(BulkWriteResult.acknowledged(INSERT, 1, 0, []), new IndexMap.RangeBased(0, 1).add(0, 0))

        when:
        def result = combiner.getResult()

        then:
        result == BulkWriteResult.unacknowledged()
    }

    def 'should get correct result for an insert'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED)
        combiner.addResult(BulkWriteResult.acknowledged(INSERT, 1, 0, []), new IndexMap.RangeBased().add(0, 0))

        when:
        def result = combiner.getResult()

        then:
        result == BulkWriteResult.acknowledged(INSERT, 1, 0, [])
    }

    def 'should handle null modifiedCount'() {
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED)
        combiner.addResult(BulkWriteResult.acknowledged(UPDATE, 1, null, []), new IndexMap.RangeBased().add(0, 0))
        combiner.addResult(BulkWriteResult.acknowledged(INSERT, 1, 0, []), new IndexMap.RangeBased().add(0, 0))

        when:
        def result = combiner.getResult()

        then:
        result == BulkWriteResult.acknowledged(1, 1, 0, null, [])
    }

    def 'should sort upserts'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED)
        combiner.addResult(BulkWriteResult.acknowledged(UPDATE, 1, 0, [new BulkWriteUpsert(0, new BsonString('id1'))]),
                           new IndexMap.RangeBased().add(0, 6))
        combiner.addResult(BulkWriteResult.acknowledged(UPDATE, 1, 0, [new BulkWriteUpsert(0, new BsonString('id2'))]),
                           new IndexMap.RangeBased().add(0, 3))

        when:
        def result = combiner.getResult()

        then:
        result == BulkWriteResult.acknowledged(UPDATE, 2, 0,
                                                  [new BulkWriteUpsert(3, new BsonString('id2')),
                                                   new BulkWriteUpsert(6, new BsonString('id1'))])
    }

    def 'should throw exception on write error'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED)

        def error = new BulkWriteError(11000, 'dup key', new BsonDocument(), 0)
        combiner.addWriteErrorResult(error, new IndexMap.RangeBased().add(0, 0))

        when:
        combiner.getResult()

        then:
        def e = thrown(MongoBulkWriteException)
        e == new MongoBulkWriteException(BulkWriteResult.acknowledged(INSERT, 0, 0, []), [error], null, new ServerAddress())
    }

    def 'should throw last write concern error'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED)
        combiner.addWriteConcernErrorResult(new WriteConcernError(65, 'journal error', new BsonDocument()));
        def writeConcernError = new WriteConcernError(75, 'wtimeout', new BsonDocument())
        combiner.addWriteConcernErrorResult(writeConcernError)

        when:
        combiner.getResult()

        then:
        def e = thrown(MongoBulkWriteException)
        e == new MongoBulkWriteException(BulkWriteResult.acknowledged(INSERT, 0, 0, []), [], writeConcernError, new ServerAddress())
    }

    def 'should not stop run if no errors'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED)
        combiner.addResult(BulkWriteResult.acknowledged(INSERT, 1, 0, []), new IndexMap.RangeBased().add(0, 0))

        expect:
        !combiner.shouldStopSendingMoreBatches()
    }

    def 'should stop run on error if ordered'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), true, ACKNOWLEDGED)
        combiner.addWriteErrorResult(new BulkWriteError(11000, 'dup key', new BsonDocument(), 0), new IndexMap.RangeBased().add(0, 0))

        expect:
        combiner.shouldStopSendingMoreBatches()
    }

    def 'should not stop run on error if unordered'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), false, ACKNOWLEDGED)
        combiner.addWriteErrorResult(new BulkWriteError(11000, 'dup key', new BsonDocument(), 0), new IndexMap.RangeBased().add(0, 0))

        expect:
        !combiner.shouldStopSendingMoreBatches()
    }

    def 'should sort errors by first index'() {
        given:
        def combiner = new BulkWriteBatchCombiner(new ServerAddress(), false, ACKNOWLEDGED)
        combiner.addErrorResult([new BulkWriteError(11000, 'dup key', new BsonDocument(), 1),
                                 new BulkWriteError(45, 'wc error', new BsonDocument(), 0)],
                                null, new IndexMap.RangeBased().add(0, 0).add(1, 1).add(2, 2));

        when:
        combiner.getResult();

        then:
        def e = thrown(MongoBulkWriteException)
        e.writeErrors == [new BulkWriteError(45, 'wc error', new BsonDocument(), 0),
                          new BulkWriteError(11000, 'dup key', new BsonDocument(), 1)]
    }
}
