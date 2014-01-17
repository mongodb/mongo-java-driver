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

import spock.lang.Subject

@SuppressWarnings('deprecated')
class MapReduceOutputSpecification extends FunctionalSpecification {
    //example response:
//    CommandResult{
//        address=localhost:27017,
//        response={ "result" : { "db":"output-1383912431569888000",
//                                "collection" : "jmr1_out"
//                              },
//                   "timeMillis" : 2774,
//                   "timing" : { "mapTime" : 0,
//                                "emitLoop" : 2755,
//                                "reduceTime" : 15,
//                                "mode" : "mixed",
//                                "total" : 2774 },
//                   "counts" : { "input" : 3,
//                                "emit" : 6,
//                                "reduce" : 2,
//                                "output" : 4 },
//                   "ok" : 1.0 },
//        elapsedNanoseconds=2777341000}


    def 'should return the name of the collection the results are contained in'() throws Exception {
        given:
        String expectedCollectionName = 'collectionForResults';
        DBObject result = new BasicDBObject('db', 'databaseName')
                .append('collection', expectedCollectionName);
        CommandResult commandResult = new CommandResult(new ServerAddress());
        commandResult.append('result', result);

        @Subject MapReduceOutput mapReduceOutput = new MapReduceOutput(this.collection, new BasicDBObject(), commandResult);

        when:
        String collectionName = mapReduceOutput.getCollectionName();

        then:
        collectionName != null
        collectionName == expectedCollectionName
    }


    def 'should return the name of the datbase the results are contained in'() throws Exception {
        given:
        String expectedDatabaseName = 'databaseName'
        DBObject result = new BasicDBObject('db', expectedDatabaseName)
                .append('collection', 'collectionForResults');
        CommandResult commandResult = new CommandResult(new ServerAddress());
        commandResult.append('result', result);

        @Subject MapReduceOutput mapReduceOutput = new MapReduceOutput(this.collection, new BasicDBObject(), commandResult);

        when:
        String databaseName = mapReduceOutput.getDatabaseName();

        then:
        databaseName != null
        databaseName == expectedDatabaseName
    }

    def 'should return the duration'() {
        given:
        int expectedDuration = 2774
        CommandResult commandResult = new CommandResult(new ServerAddress());
        commandResult.append('timeMillis', expectedDuration);
        commandResult.append('result', 'stubCollectionName');

        @Subject MapReduceOutput mapReduceOutput = new MapReduceOutput(this.collection, new BasicDBObject(), commandResult);

        when:
        int duration = mapReduceOutput.getDuration();

        then:
        duration != null
        duration == expectedDuration
    }

    def 'should return the count values'() {
        given:
        int expectedInputCount = 3
        int expectedOutputCount = 4
        int expectedEmitCount = 6
        CommandResult commandResult = new CommandResult(new ServerAddress());
        DBObject counts = new BasicDBObject();
        counts.append('input', expectedInputCount)
        counts.append('output', expectedOutputCount)
        counts.append('emit', expectedEmitCount)
        commandResult.append('counts', counts);
        commandResult.append('result', 'stubCollectionName');

        @Subject MapReduceOutput mapReduceOutput = new MapReduceOutput(this.collection, new BasicDBObject(), commandResult);

        expect:
        mapReduceOutput.getInputCount() == expectedInputCount
        mapReduceOutput.getOutputCount() == expectedOutputCount
        mapReduceOutput.getEmitCount() == expectedEmitCount
    }

}
