/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal.connection

import com.mongodb.ReadConcern
import com.mongodb.internal.session.SessionContext
import org.bson.BsonDocument
import org.bson.BsonString
import org.bson.BsonTimestamp
import spock.lang.Specification

import static com.mongodb.internal.connection.ReadConcernHelper.getReadConcernDocument
import static com.mongodb.internal.operation.ServerVersionHelper.UNKNOWN_WIRE_VERSION

class ReadConcernHelperSpecification extends Specification {

    def 'should throw IllegalArgumentException if session context is null'() {
        when:
        getReadConcernDocument(null, UNKNOWN_WIRE_VERSION)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should add afterClusterTime to majority read concern when session is causally consistent'() {
        given:
        def operationTime = new BsonTimestamp(42, 1)
        def sessionContext = Stub(SessionContext) {
            isCausallyConsistent() >> true
            getOperationTime() >> operationTime
            getReadConcern() >> ReadConcern.MAJORITY
        }

        expect:
        getReadConcernDocument(sessionContext, UNKNOWN_WIRE_VERSION) == new BsonDocument('level', new BsonString('majority'))
                .append('afterClusterTime', operationTime)
    }

    def 'should add afterClusterTime to default read concern when session is causally consistent'() {
        given:
        def operationTime = new BsonTimestamp(42, 1)
        def sessionContext = Stub(SessionContext) {
            isCausallyConsistent() >> true
            getOperationTime() >> operationTime
            getReadConcern() >> ReadConcern.DEFAULT
        }

        expect:
        getReadConcernDocument(sessionContext, UNKNOWN_WIRE_VERSION) ==
                new BsonDocument(new BsonDocument('afterClusterTime', operationTime))
    }

    def 'should not add afterClusterTime to ReadConcern when session is not causally consistent'() {
        given:
        def sessionContext = Stub(SessionContext) {
            isCausallyConsistent() >> false
            getOperationTime() >> { throw new UnsupportedOperationException() }
            getReadConcern() >> ReadConcern.MAJORITY
        }

        expect:
        getReadConcernDocument(sessionContext, UNKNOWN_WIRE_VERSION) == new BsonDocument('level', new BsonString('majority'))
    }

    def 'should not add afterClusterTime to ReadConcern when operation time is null'() {
        given:
        def sessionContext = Stub(SessionContext) {
            isCausallyConsistent() >> true
            getOperationTime() >> null
            getReadConcern() >> ReadConcern.MAJORITY
        }

        expect:
        getReadConcernDocument(sessionContext, UNKNOWN_WIRE_VERSION) == new BsonDocument('level', new BsonString('majority'))
    }
}
