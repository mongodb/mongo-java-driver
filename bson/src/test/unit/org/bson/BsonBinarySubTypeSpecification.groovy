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

package org.bson

import spock.lang.Specification

class BsonBinarySubTypeSpecification extends Specification {

    def 'should be uuid only for legacy and uuid types'() {
        expect:
        BsonBinarySubType.isUuid(value as byte) == isUuid

        where:
        value | isUuid
        1     | false
        2     | false
        3     | true
        4     | true
        5     | false
        6     | false
        7     | false
        8     | false
        9     | false
    }
}
