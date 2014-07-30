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

package org.mongodb

import com.mongodb.operation.Index
import org.bson.BsonDocument
import org.bson.BsonString
import spock.lang.Specification
import spock.lang.Unroll

class IndexSpecification extends Specification {
    @Unroll
    def 'should generate index name #indexName for #index'() {
        expect:
        index.getName() == indexName;

        where:
        index                                                       | indexName
        Index.builder().addKey('x').build()                         | 'x_1'
        Index.builder().addKey('x', com.mongodb.operation.OrderBy.ASC).build()            | 'x_1'
        Index.builder().addKey('x', com.mongodb.operation.OrderBy.DESC).build()           | 'x_-1'
        Index.builder().addKey(new Index.GeoKey('x')).build()       | 'x_2d'
        Index.builder().addKey(new Index.GeoSphereKey('x')).build() | 'x_2dsphere'

        Index.builder().addKeys(
                new Index.OrderedKey('x', com.mongodb.operation.OrderBy.ASC),
                new Index.OrderedKey('y', com.mongodb.operation.OrderBy.ASC),
                new Index.OrderedKey('a', com.mongodb.operation.OrderBy.ASC)).build()     | 'x_1_y_1_a_1'

        Index.builder().addKeys(
                new Index.GeoKey('x'),
                new Index.OrderedKey('y', com.mongodb.operation.OrderBy.DESC)).build()    | 'x_2d_y_-1'

    }

    @Unroll
    def 'should support unique indexes'() {
        expect:
        index.isUnique() == isUnique

        where:
        index                                                      | isUnique
        Index.builder().addKey('x').build()                        | false
        Index.builder().addKey('x').unique().build()               | true
        Index.builder().addKey('x').unique().unique(false).build() | false
    }

    @Unroll
    def 'should support ttl indexes'() {
        expect:
        index.getExpireAfterSeconds() == seconds

        where:
        index                                                        | seconds
        Index.builder().addKey('x').build()                          | -1
        Index.builder().addKey('x').expireAfterSeconds(1000).build() | 1000
        Index.builder().addKey('x').expireAfterSeconds(1000)
             .expireAfterSeconds(-1).build()                         | -1
    }

    @Unroll
    def 'should support dropping duplicates'() {
        expect:
        index.isDropDups() == dropDups

        where:
        index                                                          | dropDups
        Index.builder().addKey('x').build()                            | false
        Index.builder().addKey('x').dropDups().build()                 | true
        Index.builder().addKey('x').dropDups(false).build()            | false
        Index.builder().addKey('x').dropDups().dropDups(false).build() | false
    }

    @Unroll
    def 'should support unknown attributes'() {
        expect:
        index.getExtra() == extra

        where:
        index                                                                     | extra
        Index.builder().addKey('x').build()                                       | new BsonDocument()
        Index.builder().addKey('x')
             .extra(new BsonDocument('extra', new BsonString('special'))).build() | new BsonDocument('extra': new BsonString('special'))
    }
}
