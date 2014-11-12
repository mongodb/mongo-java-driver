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

import com.mongodb.operation.MapReduceBatchCursor
import com.mongodb.operation.MapReduceStatistics
import spock.lang.Subject

@SuppressWarnings('deprecated')
class MapReduceOutputSpecification extends FunctionalSpecification {
    //example response:
//    CommandResult{
//        address=localhost:27017,
//        response={ 'result' : { 'db':'output-1383912431569888000',
//                                'collection' : 'jmr1_out'
//                              },
//                   'timeMillis' : 2774,
//                   'timing' : { 'mapTime' : 0,
//                                'emitLoop' : 2755,
//                                'reduceTime' : 15,
//                                'mode' : 'mixed',
//                                'total' : 2774 },
//                   'counts' : { 'input' : 3,
//                                'emit' : 6,
//                                'reduce' : 2,
//                                'output' : 4 },
//                   'ok' : 1.0 },
//        elapsedNanoseconds=2777341000}


    def 'should return the name of the collection the results are contained in if it is not inline'() {
        given:
        def expectedCollectionName = 'collectionForResults';
        def outputCollection = database.getCollection(expectedCollectionName)
        def results = outputCollection.find()

        @Subject
        def mapReduceOutput = new MapReduceOutput(new BasicDBObject(), results, null, outputCollection);

        when:
        def collectionName = mapReduceOutput.getCollectionName();

        then:
        collectionName != null
        collectionName == expectedCollectionName
    }

    def 'should return null for the name of the collection if it is inline'() {
        given:
        MapReduceBatchCursor mongoCursor = Mock();

        @Subject
        def mapReduceOutput = new MapReduceOutput(new BasicDBObject(), mongoCursor);

        when:
        def collectionName = mapReduceOutput.getCollectionName();

        then:
        collectionName == null
    }

    def 'should return the name of the database the results are contained in if it is not inline'() {
        given:
        def expectedDatabaseName = databaseName
        def expectedCollectionName = 'collectionForResults';
        def outputCollection = database.getCollection(expectedCollectionName)
        def results = outputCollection.find()

        @Subject
        def mapReduceOutput = new MapReduceOutput(new BasicDBObject(), results, null, outputCollection);

        when:
        def databaseName = mapReduceOutput.getDatabaseName();

        then:
        databaseName != null
        databaseName == expectedDatabaseName
    }

    def 'should return the duration for a map-reduce into a collection'() {
        given:
        def expectedDuration = 2774

        MapReduceStatistics mapReduceStats = Mock();
        mapReduceStats.getDuration() >> expectedDuration

        @Subject
        def mapReduceOutput = new MapReduceOutput(new BasicDBObject(), null, mapReduceStats, null);

        expect:
        mapReduceOutput.getDuration() == expectedDuration;
    }

    def 'should return the duration for an inline map-reduce'() {
        given:
        def expectedDuration = 2774

        MapReduceBatchCursor mongoCursor = Mock();
        mongoCursor.getStatistics() >> new MapReduceStatistics(5, 10, 5, expectedDuration)

        @Subject
        def mapReduceOutput = new MapReduceOutput(new BasicDBObject(), mongoCursor);

        expect:
        mapReduceOutput.getDuration() == expectedDuration
    }

    def 'should return the count values for a map-reduce into a collection'() {
        given:
        def expectedInputCount = 3
        def expectedOutputCount = 4
        def expectedEmitCount = 6

        MapReduceStatistics mapReduceStats = new MapReduceStatistics(expectedInputCount, expectedOutputCount, expectedEmitCount, 5)

        @Subject
        def mapReduceOutput = new MapReduceOutput(new BasicDBObject(), null, mapReduceStats, null);

        expect:
        mapReduceOutput.getInputCount() == expectedInputCount
        mapReduceOutput.getOutputCount() == expectedOutputCount
        mapReduceOutput.getEmitCount() == expectedEmitCount
    }

    def 'should return the count values for an inline map-reduce output'() {
        given:
        def expectedInputCount = 3
        def expectedOutputCount = 4
        def expectedEmitCount = 6
        def expectedDuration = 10

        MapReduceBatchCursor mapReduceCursor = Mock();
        mapReduceCursor.getStatistics() >> new MapReduceStatistics(expectedInputCount, expectedOutputCount, expectedEmitCount,
                                                                   expectedDuration)

        @Subject
        def mapReduceOutput = new MapReduceOutput(new BasicDBObject(), mapReduceCursor);

        expect:
        mapReduceOutput.getInputCount() == expectedInputCount
        mapReduceOutput.getOutputCount() == expectedOutputCount
        mapReduceOutput.getEmitCount() == expectedEmitCount
    }
}
