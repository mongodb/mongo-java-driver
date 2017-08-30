/*
 * Copyright 2017 MongoDB, Inc.
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
 *
 */

package com.mongodb.internal.connection

import org.bson.BsonDocument
import org.bson.BsonTimestamp
import spock.lang.Specification

class NoOpSessionContextSpecification extends Specification {
    def 'should be a no-op'() {
        given:
        def sessionContext = NoOpSessionContext.INSTANCE

        expect:
        !sessionContext.hasSession()
        sessionContext.getClusterTime() == null

        when:
        sessionContext.advanceOperationTime(new BsonTimestamp(42, 1))

        then:
        true

        when:
        sessionContext.advanceClusterTime(new BsonDocument())

        then:
        true

        when:
        sessionContext.getSessionId()

        then:
        thrown(UnsupportedOperationException)

        when:
        sessionContext.advanceTransactionNumber()

        then:
        thrown(UnsupportedOperationException)
    }
}
