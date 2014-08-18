/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

package com.mongodb
import com.mongodb.operation.MapReduce
import com.mongodb.operation.MapReduceOutputOptions
import org.bson.BsonDocument
import org.bson.BsonInt32
import org.bson.BsonJavaScript
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.TimeUnit

class MapReduceSpecification extends Specification {

    private static BsonJavaScript map
    private static BsonJavaScript reduce
    private static MapReduceOutputOptions output
    private static MapReduce mapReduceA
    private static MapReduce mapReduceB

    def setupSpec() {
        map = new BsonJavaScript('map')
        reduce = new BsonJavaScript('reduce')
        output = new MapReduceOutputOptions('test')
        mapReduceA = new MapReduce(map, reduce, null)
                .finalize(map)
                .filter(new BsonDocument())
                .sort(new BsonDocument('a', new BsonInt32(1)))
                .maxTime(1, TimeUnit.SECONDS)
        mapReduceB = new MapReduce(map, reduce, output)
                        .finalize(map)
                        .filter(new BsonDocument())
                        .sort(new BsonDocument('a', new BsonInt32(1)))
                        .maxTime(1, TimeUnit.SECONDS)
    }

    def 'default MapReduce is as expected'() {
        when:
        def mr = new MapReduce(map, reduce)

        then:
        mr.isInline()
        !mr.isJsMode()
        !mr.isVerbose()
        mr.getMaxTime(TimeUnit.MILLISECONDS) == 0
        mr.getFilter() == null
        mr.getFinalizeFunction() == null
        mr.getLimit() == 0
        mr.getMapFunction() == map
        mr.getOutput() == null
        mr.getReduceFunction() == reduce
        mr.getScope() == null
        mr.getSortCriteria() == null
    }

    @Unroll
    def 'constructors should set up the #n map reduce correctly'() {
        expect:
        mr.getMapFunction() == m
        mr.getReduceFunction() == r
        mr.isInline() == inline
        mr.getOutput() == options

        where:
        n               | mr                                    | m    | r      | inline | options
        'simple'        | new MapReduce(map, reduce)            | map  | reduce | true   | null
        'null options'  | new MapReduce(map, reduce, null)      | map  | reduce | true   | null
        'options'       | new MapReduce(map, reduce, output)    | map  | reduce | false  | output
    }

    @SuppressWarnings('ExplicitCallToEqualsMethod')
    def 'test equals'() {
        expect:
        mr.equals(compareTo) == expectedResult

        where:
        mr                                      | compareTo                              | expectedResult
        new MapReduce(map, reduce)              | new MapReduce(map, reduce)             | true
        new MapReduce(map, reduce, null)        | new MapReduce(map, reduce, null)       | true
        new MapReduce(map, reduce, output)      | new MapReduce(map, reduce, output)     | true
        new MapReduce(map, reduce)              | new MapReduce(map, reduce, output)     | false
        new MapReduce(map, reduce, null)        | new MapReduce(map, reduce, output)     | false
        mapReduceA                              | mapReduceA                             | true
        mapReduceA                              | mapReduceB                             | false
        new MapReduce(map, reduce, null)
          .jsMode().verbose().finalize(map)     |  new MapReduce(map, reduce)
                                                    .jsMode().verbose().finalize(map)    | true
        new MapReduce(map, reduce, null)
            .jsMode().verbose().finalize(map)   |  new MapReduce(map, reduce)
                                                    .jsMode().verbose().finalize(reduce) | false

    }

}
