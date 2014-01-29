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

import org.bson.types.ObjectId
import org.mongodb.CommandResult
import org.mongodb.Document
import org.mongodb.MongoCommandFailureException
import org.mongodb.MongoDuplicateKeyException
import org.mongodb.connection.ServerAddress
import spock.lang.Specification

import static java.util.Arrays.asList

class WriteResultProtocolHelperSpecification extends Specification {

   def 'should return a write result for an insert'() {
        given:
        def commandResult = new CommandResult(new ServerAddress(),
                                              new Document('ok', 1).append('n', 0),
                                              1);
        when:
        def writeResult = ProtocolHelper.getWriteResult(commandResult)

        then:
        writeResult == new AcknowledgedWriteResult(0, false, null)

    }

    def 'should return a write result for an upsert'() {
        given:
        def id = new ObjectId()
        def commandResult = new CommandResult(new ServerAddress(),
                                              new Document('ok', 1).append('n', 1).append('updatedExisting', false).append('upserted', id),
                                              1);
        when:
        def writeResult = ProtocolHelper.getWriteResult(commandResult)

        then:
        writeResult == new AcknowledgedWriteResult(1, false, id)

    }

    def 'should throw command failure if result is not ok'() {
        given:
        def commandResult = new CommandResult(new ServerAddress(),
                                              new Document('ok', 0).append('errmsg', 'Something is very wrong').append('code', 14),
                                              1);
        when:
        ProtocolHelper.getWriteResult(commandResult)

        then:
        def e = thrown(MongoCommandFailureException)
        e.getErrorCode() == 14
    }

    def 'should throw duplicate key when response has a duplicate key error code'() {
        given:
        def commandResult = new CommandResult(new ServerAddress(),
                                              new Document('ok', 1)
                                                      .append('err', 'E11000 duplicate key error index 1')
                                                      .append('code', 11000),
                                              1);

        when:
        ProtocolHelper.getWriteResult(commandResult)

        then:
        def e = thrown(MongoDuplicateKeyException)
        e.getErrorCode() == 11000
    }

    def 'should throw duplicate key when errObjects has a duplicate key error code'() {
        given:
        def commandResult = new CommandResult(new ServerAddress(),
                                              new Document('ok', 1)
                                                      .append('err', 'E11000 duplicate key error index 1')
                                                      .append('errObjects',
                                                              asList(new Document('ok', 1)
                                                                             .append('err', 'E11000 duplicate key error index 1')
                                                                             .append('code', 11000),
                                                                     new Document('ok', 1)
                                                                             .append('err', 'E11000 duplicate key error index 2')
                                                                             .append('code', 11000))),
                                              1);

        when:
        ProtocolHelper.getWriteResult(commandResult)

        then:
        def e = thrown(MongoDuplicateKeyException)
        e.getErrorCode() == 11000
    }
}
