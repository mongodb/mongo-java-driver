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

import org.bson.BsonDocument
import org.bson.BsonTimestamp
import spock.lang.Specification


class ClusterClockSpecification extends Specification {
    def 'should advance cluster time'() {
        given:
        def firstClusterTime = new BsonDocument('clusterTime', new BsonTimestamp(42, 1))
        def secondClusterTime = new BsonDocument('clusterTime', new BsonTimestamp(52, 1))
        def olderClusterTime = new BsonDocument('clusterTime', new BsonTimestamp(22, 1))

        when:
        def clock = new ClusterClock()

        then:
        clock.getCurrent() == null

        when:
        clock.advance(null)

        then:
        clock.getCurrent() == null
        clock.greaterOf(firstClusterTime) == firstClusterTime

        when:
        clock.advance(firstClusterTime)

        then:
        clock.getCurrent() == firstClusterTime
        clock.greaterOf(secondClusterTime) == secondClusterTime

        when:
        clock.advance(secondClusterTime)

        then:
        clock.getCurrent() == secondClusterTime
        clock.greaterOf(olderClusterTime) == secondClusterTime

        when:
        clock.advance(olderClusterTime)

        then:
        clock.getCurrent() == secondClusterTime
    }
}
