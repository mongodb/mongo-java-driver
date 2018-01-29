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

package com.mongodb

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import static com.mongodb.MapReduceCommand.OutputType
import static com.mongodb.ReadPreference.primary
import static java.util.concurrent.TimeUnit.SECONDS

class MapReduceCommandSpecification extends Specification {
    @Shared
    private MapReduceCommand cmd
    private static final String COLLECTION_NAME = 'collectionName'

    def mapReduceCommand() {
        def collection = Mock(DBCollection) {
            getName() >> { COLLECTION_NAME }
        }
        new MapReduceCommand(collection, 'map', 'reduce', 'test', OutputType.REDUCE, new BasicDBObject())
    }

    def sort() { ['a': 1] as BasicDBObject }

    def scope() { ['a': 'b'] }

    def setupSpec() { cmd = mapReduceCommand() }

    @Unroll
    def 'should have the correct default for #field'() throws Exception {
        expect:
        value == expected

        where:
        field            | value                   | expected
        'finalize'       | cmd.getFinalize()       | null
        'input'          | cmd.getInput()          | COLLECTION_NAME
        'jsMode'         | cmd.getJsMode()         | null
        'limit'          | cmd.getLimit()          | 0
        'map'            | cmd.getMap()            | 'map'
        'maxTime'        | cmd.getMaxTime(SECONDS) | 0
        'output db'      | cmd.getOutputDB()       | null
        'output target'  | cmd.getOutputTarget()   | 'test'
        'output type'    | cmd.getOutputType()     | OutputType.REDUCE
        'query'          | cmd.getQuery()          | new BasicDBObject()
        'readPreference' | cmd.getReadPreference() | null
        'reduce'         | cmd.getReduce()         | 'reduce'
        'scope'          | cmd.getScope()          | null
        'sort'           | cmd.getSort()           | null
        'verbose'        | cmd.isVerbose()         | true
    }

    @Unroll
    def 'should be able to change the default for #field'() throws Exception {
        expect:
        value == expected

        where:
        field            | change                           | value                   | expected
        'finalize'       | cmd.setFinalize('final')         | cmd.getFinalize()       | 'final'
        'jsMode'         | cmd.setJsMode(true)              | cmd.getJsMode()         | true
        'limit'          | cmd.setLimit(100)                | cmd.getLimit()          | 100
        'maxTime'        | cmd.setMaxTime(1, SECONDS)       | cmd.getMaxTime(SECONDS) | 1
        'output db'      | cmd.setOutputDB('outDB')         | cmd.getOutputDB()       | 'outDB'
        'readPreference' | cmd.setReadPreference(primary()) | cmd.getReadPreference() | primary()
        'scope'          | cmd.setScope(scope())            | cmd.getScope()          | scope()
        'sort'           | cmd.setSort(sort())              | cmd.getSort()           | sort()
        'verbose'        | cmd.setVerbose(false)            | cmd.isVerbose()         | false
    }

    def 'should produce the expected DBObject when changed'() throws Exception {
        given:
        cmd.with {
            setFinalize('final')
            setJsMode(true)
            setLimit(100)
            setMaxTime(1, SECONDS)
            setOutputDB('outDB')
            setReadPreference(primary())
            setScope(scope())
            setSort(sort())
            setVerbose(false)
        }

        when:
        def expected = [mapreduce: COLLECTION_NAME, map: 'map', reduce: 'reduce', verbose: false,
                        out      : [reduce: 'test', db: 'outDB'] as BasicDBObject, query: [:] as BasicDBObject,
                        finalize : 'final', sort: sort(), limit: 100, scope: scope(), jsMode: true,
                        maxTimeMS: 1000] as BasicDBObject

        then:
        cmd.toDBObject() == expected
    }

}
