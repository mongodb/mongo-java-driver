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

import spock.lang.Specification

class GroupCommandSpecification extends Specification {
    def setupSpec() {
        Map.metaClass.bitwiseNegate = { new BasicDBObject(delegate) }
    }

    @SuppressWarnings('DuplicateMapLiteral')
    def 'should convert to DBObject'() {
        given:
        DBCollection collection = Mock(DBCollection)
        GroupCommand command = new GroupCommand(collection,
                                                ~['x': 1],
                                                null,
                                                ~['c': 0],
                                                'function(o, p){}',
                                                null)

        when:
        DBObject dbObject = command.toDBObject()

        DBObject args = ~['ns'     : collection.getName(), 'key': ~['x': 1],
                          'cond'   : null,
                          '$reduce': 'function(o, p){}',
                          'initial': ~['c': 0]];

        then:
        dbObject == ~['group': args];
    }

}
