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

import org.bson.types.ObjectId

class LazyDBObjectSpecification extends FunctionalSpecification {

    def 'should understand DBRefs'() {
        given:
        byte[] bytes = [
                44, 0, 0, 0, 3, 102, 0, 36, 0, 0, 0, 2, 36, 114, 101, 102,
                0, 4, 0, 0, 0, 97, 46, 98, 0, 7, 36, 105, 100, 0, 18, 52,
                86, 120, -112, 18, 52, 86, 120, -112, 18, 52, 0, 0,
        ]

        when:
        LazyDBObject document = new LazyDBObject(bytes, new LazyDBCallback(collection))

        then:
        document.get('f') instanceof DBRef
        document.get('f') == new DBRef(database, 'a.b', new ObjectId('123456789012345678901234'))

    }
}
