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

import spock.lang.Specification

import static com.mongodb.WriteConcern.ACKNOWLEDGED
import static com.mongodb.WriteConcern.UNACKNOWLEDGED
import static com.mongodb.WriteRequest.Type.INSERT
import static com.mongodb.WriteRequest.Type.UPDATE

class BulkWriteBatchCombinerSpecification extends Specification {
    def 'should get unacknowledged result for an unacknowledged write'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), UNACKNOWLEDGED)
        runResults.addResult(new AcknowledgedBulkWriteResult(INSERT, 1, 0, []), new IndexMap.RangeBased(0, 1).add(0, 0))

        when:
        def result = runResults.getResult()

        then:
        result == new UnacknowledgedBulkWriteResult()
    }

    def 'should get correct result for an insert'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED)
        runResults.addResult(new AcknowledgedBulkWriteResult(INSERT, 1, 0, []), new IndexMap.RangeBased().add(0, 0))

        when:
        def result = runResults.getResult()

        then:
        result == new AcknowledgedBulkWriteResult(INSERT, 1, 0, [])
    }

    def 'should handle null modifiedCount'() {
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED)
        runResults.addResult(new AcknowledgedBulkWriteResult(UPDATE, 1, null, []), new IndexMap.RangeBased().add(0, 0))
        runResults.addResult(new AcknowledgedBulkWriteResult(INSERT, 1, 0, []), new IndexMap.RangeBased().add(0, 0))

        when:
        def result = runResults.getResult()

        then:
        result == new AcknowledgedBulkWriteResult(1, 1, 0, null, [])
    }

    def 'should sort upserts'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED)
        runResults.addResult(new AcknowledgedBulkWriteResult(UPDATE, 1, 0, [new BulkWriteUpsert(0, 'id1')]),
                             new IndexMap.RangeBased().add(0, 6))
        runResults.addResult(new AcknowledgedBulkWriteResult(UPDATE, 1, 0, [new BulkWriteUpsert(0, 'id2')]),
                             new IndexMap.RangeBased().add(0, 3))

        when:
        def result = runResults.getResult()

        then:
        result == new AcknowledgedBulkWriteResult(UPDATE, 2, 0,
                                                  [new BulkWriteUpsert(3, 'id2'),
                                                   new BulkWriteUpsert(6, 'id1')])
    }

    def 'should throw exception on write error'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED)

        def error = new BulkWriteError(11000, 'dup key', new BasicDBObject(), 0)
        runResults.addWriteErrorResult(error, new IndexMap.RangeBased().add(0, 0))

        when:
        runResults.getResult()

        then:
        def e = thrown(BulkWriteException)
        e == new BulkWriteException(new AcknowledgedBulkWriteResult(INSERT, 0, 0, []), [error], null, new ServerAddress())
    }

    def 'should throw last write concern error'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED)
        runResults.addWriteConcernErrorResult(new WriteConcernError(65, 'journal error', new BasicDBObject()));
        def writeConcernError = new WriteConcernError(75, 'wtimeout', new BasicDBObject())
        runResults.addWriteConcernErrorResult(writeConcernError)

        when:
        runResults.getResult()

        then:
        def e = thrown(BulkWriteException)
        e == new BulkWriteException(new AcknowledgedBulkWriteResult(INSERT, 0, 0, []), [], writeConcernError, new ServerAddress())
    }

    def 'should not stop run if no errors'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED)
        runResults.addResult(new AcknowledgedBulkWriteResult(INSERT, 1, 0, []), new IndexMap.RangeBased().add(0, 0))

        expect:
        !runResults.shouldStopSendingMoreBatches()
    }

    def 'should stop run on error if ordered'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED)
        runResults.addWriteErrorResult(new BulkWriteError(11000, 'dup key', new BasicDBObject(), 0), new IndexMap.RangeBased().add(0, 0))

        expect:
        runResults.shouldStopSendingMoreBatches()
    }

    def 'should not stop run on error if unordered'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED.continueOnError(true))
        runResults.addWriteErrorResult(new BulkWriteError(11000, 'dup key', new BasicDBObject(), 0), new IndexMap.RangeBased().add(0, 0))

        expect:
        !runResults.shouldStopSendingMoreBatches()
    }

    def 'should sort errors by first index'() {
        given:
        def runResults = new BulkWriteBatchCombiner(new ServerAddress(), ACKNOWLEDGED.continueOnError(true))
        runResults.addErrorResult([new BulkWriteError(11000, 'dup key', new BasicDBObject(), 1),
                                   new BulkWriteError(45, 'wc error', new BasicDBObject(), 0)],
                       null, new IndexMap.RangeBased().add(0, 0).add(1, 1).add(2, 2));

        when:
        runResults.getResult();

        then:
        def e = thrown(BulkWriteException)
        e.writeErrors == [new BulkWriteError(45, 'wc error', new BasicDBObject(), 0),
                     new BulkWriteError(11000, 'dup key', new BasicDBObject(), 1)]
    }
}