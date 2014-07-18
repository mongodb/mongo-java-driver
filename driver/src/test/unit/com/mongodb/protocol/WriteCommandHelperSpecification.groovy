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

package com.mongodb.protocol

import com.mongodb.MongoInternalException
import com.mongodb.ServerAddress
import org.bson.BsonArray
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonString
import org.mongodb.BulkWriteError
import org.mongodb.BulkWriteUpsert
import org.mongodb.CommandResult
import org.mongodb.WriteConcernError
import spock.lang.Specification

import static com.mongodb.protocol.WriteCommandResultHelper.getBulkWriteException
import static com.mongodb.protocol.WriteCommandResultHelper.getBulkWriteResult
import static com.mongodb.protocol.WriteCommandResultHelper.hasError
import static org.mongodb.operation.WriteRequest.Type.INSERT
import static org.mongodb.operation.WriteRequest.Type.REMOVE
import static org.mongodb.operation.WriteRequest.Type.REPLACE
import static org.mongodb.operation.WriteRequest.Type.UPDATE

class WriteCommandHelperSpecification extends Specification {

    def 'should get bulk write result from with a count matching the n field'() {
        expect:
        getBulkWriteResult(INSERT, getCommandResult(new BsonDocument('n', new BsonInt32(1)))) ==
        new AcknowledgedBulkWriteResult(INSERT, 1, [])
    }


    def 'should get bulk write result with upserts matching the upserted field'() {
        expect:
        [new BulkWriteUpsert(0, new BsonString('id1')), new BulkWriteUpsert(2, new BsonString('id2'))] ==
        getBulkWriteResult(UPDATE, getCommandResult(new BsonDocument('n', new BsonInt32(1))
                                                            .append('upserted', new BsonArray([new BsonDocument('index', new BsonInt32(0))
                                                                                                       .
                                                                                                       append('_id', new BsonString('id1')),
                                                                                               new BsonDocument('index', new BsonInt32(2))
                                                                                                       .append('_id',
                                                                                                               new BsonString('id2'))]))))
                .getUpserts()
    }


    def 'should not have modified count for update with no nModified field in the result'() {
        expect:
        !getBulkWriteResult(UPDATE, getCommandResult(new BsonDocument('n', new BsonInt32(1)))).isModifiedCountAvailable()
    }

    def 'should not have modified count for replace with no nModified field in the result'() {
        expect:
        !getBulkWriteResult(REPLACE, getCommandResult(new BsonDocument('n', new BsonInt32(1)))).isModifiedCountAvailable()
    }

    def 'should have modified count of 0 for insert with no nModified field in the result'() {
        expect:
        0 == getBulkWriteResult(INSERT, getCommandResult(new BsonDocument('n', new BsonInt32(1)))).getModifiedCount()
    }

    def 'should have modified count of 0 for remove with no nModified field in the result'() {
        expect:
        0 == getBulkWriteResult(REMOVE, getCommandResult(new BsonDocument('n', new BsonInt32(1)))).getModifiedCount()
    }

    def 'should not have error if writeErrors is empty and writeConcernError is missing'() {
        expect:
        !hasError(getCommandResult(new BsonDocument()));
    }

    def 'should have error if writeErrors is not empty'() {
        expect:
        hasError(getCommandResult(new BsonDocument('writeErrors',
                                                   new BsonArray([new BsonDocument('index', new BsonInt32(3))
                                                                          .append('code', new BsonInt32(100))
                                                                          .append('errmsg', new BsonString('some error'))]))));
    }

    def 'should have error if writeConcernError is present'() {
        expect:
        hasError(getCommandResult(new BsonDocument('writeConcernError',
                                                   new BsonDocument('code', new BsonInt32(75))
                                                           .append('errmsg', new BsonString('wtimeout'))
                                                           .append('errInfo', new BsonDocument('wtimeout', new BsonString('0'))))))
    }

    def 'getting bulk write exception should throw if there are no errors'() {
        when:
        getBulkWriteException(INSERT, getCommandResult(new BsonDocument()))

        then:
        thrown(MongoInternalException)
    }

    def 'should get write errors from the writeErrors field'() {
        expect:
        [new BulkWriteError(100, 'some error', new BsonDocument(), 3),
         new BulkWriteError(11000, 'duplicate key', new BsonDocument('_id', new BsonString('id1')), 5)] ==
        getBulkWriteException(INSERT, getCommandResult(new BsonDocument('ok', new BsonInt32(0))
                                                               .append('n', new BsonInt32(1))
                                                               .append('code', new BsonInt32(65))
                                                               .append('errmsg', new BsonString('bulk op errors'))
                                                               .append('writeErrors', new BsonArray(
                [new BsonDocument('index', new BsonInt32(3))
                         .append('code', new BsonInt32(100))
                         .append('errmsg', new BsonString('some error')),
                 new BsonDocument('index', new BsonInt32(5))
                         .append('code', new BsonInt32(11000))
                         .append('errmsg', new BsonString('duplicate key'))
                         .append('errInfo',
                                 new BsonDocument('_id',
                                                  new BsonString('id1')))]))))
                .writeErrors

    }

    def 'should get write concern error from writeConcernError field'() {
        expect:
        new WriteConcernError(75, 'wtimeout', new BsonDocument('wtimeout', new BsonString('0'))) ==
        getBulkWriteException(INSERT, getCommandResult(new BsonDocument('n', new BsonInt32(1))
                                                               .append('writeConcernError',
                                                                       new BsonDocument('code', new BsonInt32(75))
                                                                               .append('errmsg', new BsonString('wtimeout'))
                                                                               .append('errInfo', new BsonDocument('wtimeout',
                                                                                                                   new BsonString('0'))))))
                .writeConcernError
    }


    def getCommandResult(BsonDocument document) {
        new CommandResult(new ServerAddress(), document)
    }
}
