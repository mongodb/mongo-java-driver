/*
 * Copyright (c) 2008 - 2014 MongoDB Inc. <http://mongodb.com>
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

import org.mongodb.BulkWriteError
import org.mongodb.BulkWriteUpsert
import org.mongodb.CommandResult
import org.mongodb.Document
import org.mongodb.MongoInternalException
import org.mongodb.WriteConcernError
import org.mongodb.connection.ServerAddress
import spock.lang.Specification

import static org.mongodb.operation.WriteRequest.Type.INSERT
import static org.mongodb.operation.WriteRequest.Type.UPDATE
import static org.mongodb.operation.WriteRequest.Type.REMOVE
import static org.mongodb.operation.WriteRequest.Type.REPLACE
import static org.mongodb.protocol.WriteCommandResultHelper.getBulkWriteException
import static org.mongodb.protocol.WriteCommandResultHelper.getBulkWriteResult
import static org.mongodb.protocol.WriteCommandResultHelper.hasError

class WriteCommandHelperSpecification extends Specification {

    def 'should get bulk write result from with a count matching the n field'() {
        expect:
        getBulkWriteResult(INSERT, getCommandResult(new Document('n', 1))) == new AcknowledgedBulkWriteResult(INSERT, 1, [])
    }


    def 'should get bulk write result with upserts matching the upserted field'() {
        expect:
        [new BulkWriteUpsert(0, 'id1'), new BulkWriteUpsert(2, 'id2')] ==
        getBulkWriteResult(UPDATE, getCommandResult(new Document('n', 1)
                                                            .append('upserted', [new Document('index', 0).append('_id', 'id1'),
                                                                                 new Document('index', 2).append('_id', 'id2')])))
                .getUpserts()
    }


    def 'should not have modified count for update with no nModified field in the result'() {
        expect:
        !getBulkWriteResult(UPDATE, getCommandResult(new Document('n', 1))).isModifiedCountAvailable()
    }

    def 'should not have modified count for replace with no nModified field in the result'() {
        expect:
        !getBulkWriteResult(REPLACE, getCommandResult(new Document('n', 1))).isModifiedCountAvailable()
    }

    def 'should have modified count of 0 for insert with no nModified field in the result'() {
        expect:
        0 == getBulkWriteResult(INSERT, getCommandResult(new Document('n', 1))).getModifiedCount()
    }

    def 'should have modified count of 0 for remove with no nModified field in the result'() {
        expect:
        0 == getBulkWriteResult(REMOVE, getCommandResult(new Document('n', 1))).getModifiedCount()
    }

    def 'should not have error if writeErrors is empty and writeConcernError is missing'() {
        expect:
        !hasError(getCommandResult(new Document()));
    }

    def 'should have error if writeErrors is not empty'() {
        expect:
        hasError(getCommandResult(new Document('writeErrors',
                                                    [new Document('index', 3)
                                                             .append('code', 100)
                                                             .append('errmsg', 'some error')])));
    }

    def 'should have error if writeConcernError is present'() {
        expect:
        hasError(getCommandResult(new Document('writeConcernError',
                                                    new Document('code', 75)
                                                            .append('errmsg', 'wtimeout')
                                                            .append('errInfo', new Document('wtimeout', '0')))))
    }

    def 'getting bulk write exception should throw if there are no errors'() {
        when:
        getBulkWriteException(INSERT, getCommandResult(new Document()))

        then:
        thrown(MongoInternalException)
    }

    def 'should get write errors from the writeErrors field'() {
        expect:
        [new BulkWriteError(100, 'some error', new Document(), 3),
         new BulkWriteError(11000, 'duplicate key', new Document('_id', 'id1'), 5)] ==
        getBulkWriteException(INSERT, getCommandResult(new Document('ok', 0)
                                                               .append('n', 1)
                                                               .append('code', 65)
                                                               .append('errmsg', 'bulk op errors')
                                                               .append('writeErrors',
                                                                       [new Document('index', 3)
                                                                                .append('code', 100)
                                                                                .append('errmsg', 'some error'),
                                                                        new Document('index', 5)
                                                                                .append('code', 11000)
                                                                                .append('errmsg', 'duplicate key')
                                                                                .append('errInfo',
                                                                                        new Document('_id', 'id1'))]))).writeErrors

    }

    def 'should get write concern error from writeConcernError field'() {
        expect:
        new WriteConcernError(75, 'wtimeout', new Document('wtimeout', '0')) ==
        getBulkWriteException(INSERT, getCommandResult(new Document('n', 1)
                                                               .append('writeConcernError',
                                                                       new Document('code', 75)
                                                                               .append('errmsg', 'wtimeout')
                                                                               .append('errInfo', new Document('wtimeout', '0')))))
                .writeConcernError
    }


    def getCommandResult(Document document) {
        new CommandResult(new ServerAddress(), document, 1)
    }
}
