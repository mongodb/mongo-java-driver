/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model

import spock.lang.Specification

class FieldSpecification extends Specification {
    def 'should validate name'() {
        when:
        new Field(null, [1, 2, 3])

        then:
        thrown(IllegalArgumentException)

    }

    def 'should accept null values'() {
        when:
        def field = new Field('name', null)

        then:
        field.getName() == 'name'
        field.getValue() == null
    }

    def 'should accept properties'() {
        when:
        def field = new Field('name', [1, 2, 3])

        then:
        field.getName() == 'name'
        field.getValue() == [1, 2, 3]
    }
}
