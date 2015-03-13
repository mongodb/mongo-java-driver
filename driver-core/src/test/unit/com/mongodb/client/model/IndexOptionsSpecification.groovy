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

package com.mongodb.client.model

import spock.lang.Specification

import java.util.concurrent.TimeUnit

class IndexOptionsSpecification extends Specification {

    def 'should validate textIndexVersion'() {
        when:
        new IndexOptions().textVersion(1)

        then:
        notThrown(IllegalArgumentException)

        when:
        new IndexOptions().textVersion(2)

        then:
        notThrown(IllegalArgumentException)

        when:
        new IndexOptions().textVersion(3)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should validate 2dsphereIndexVersion'() {
        when:
        new IndexOptions().sphereVersion(1)

        then:
        notThrown(IllegalArgumentException)

        when:
        new IndexOptions().sphereVersion(2)

        then:
        notThrown(IllegalArgumentException)

        when:
        new IndexOptions().sphereVersion(3)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should convert expireAfter'() {
        when:
        def options = new IndexOptions()

        then:
        !options.getExpireAfter(TimeUnit.SECONDS)

        when:
        options = new IndexOptions().expireAfter(null, null)

        then:
        !options.getExpireAfter(TimeUnit.SECONDS)

        when:
        options = new IndexOptions().expireAfter(4, TimeUnit.MILLISECONDS)

        then:
        options.getExpireAfter(TimeUnit.SECONDS) == 0

        when:
        options = new IndexOptions().expireAfter(1004, TimeUnit.MILLISECONDS)

        then:
        options.getExpireAfter(TimeUnit.SECONDS) == 1

    }
}