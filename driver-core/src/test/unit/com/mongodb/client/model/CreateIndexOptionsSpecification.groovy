/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.client.model.CreateIndexOptions
import spock.lang.Specification

class CreateIndexOptionsSpecification extends Specification {

    def 'should validate textIndexVersion'() {
        when:
        new CreateIndexOptions().textIndexVersion(1)

        then:
        notThrown(IllegalArgumentException)

        when:
        new CreateIndexOptions().textIndexVersion(2)

        then:
        notThrown(IllegalArgumentException)

        when:
        new CreateIndexOptions().textIndexVersion(3)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should validate 2dsphereIndexVersion'() {
        when:
        new CreateIndexOptions().twoDSphereIndexVersion(1)

        then:
        notThrown(IllegalArgumentException)

        when:
        new CreateIndexOptions().twoDSphereIndexVersion(2)

        then:
        notThrown(IllegalArgumentException)

        when:
        new CreateIndexOptions().twoDSphereIndexVersion(3)

        then:
        thrown(IllegalArgumentException)
    }
}