package com.mongodb

import org.mongodb.operation.MapReduce
import spock.lang.Specification

@SuppressWarnings('DuplicateMapLiteral')
class MapReduceCommandSpecification extends Specification {
    //http://docs.mongodb.org/manual/reference/command/mapReduce/#mapreduce-out-cmd
//    db.runCommand(
//    {
//        mapReduce: <collection>,
//        map: <function>,
//        reduce: <function>,
//        out: <output>,
//        query: <document>,
//        sort: <document>,
//        limit: <number>,
//        finalize: <function>,
//        scope: <document>,
//        jsMode: <boolean>,
//        verbose: <boolean>
//    }
//    )

    def 'should populate map and reduce functions from command'() {
        given:
        String mapFunction = 'mapFunction'
        String reduceFunction = 'reduceFunction'
        DBObject command = new BasicDBObject('mapReduce', 'sourceCollection').append('map', mapFunction)
                                                                     .append('reduce', reduceFunction)
                                                                     .append('out', ['inline': 1] as DBObject);

        when:
        MapReduce mapReduce = MapReduceCommand.getMapReduceFromDBObject(command)

        then:
        mapReduce.mapFunction.code == mapFunction
        mapReduce.reduceFunction.code == reduceFunction
    }

    def 'should work out if the command is an inline map reduce'() {
        given:
        DBObject command = new BasicDBObject('mapReduce', 'sourceCollection').append('out', ['inline': 1] as DBObject);

        when:
        MapReduce mapReduce = MapReduceCommand.getMapReduceFromDBObject(command)

        then:
        mapReduce.inline
    }

    def 'should have the output collection name if the map reduce is not inline'() {
        given:
        String outputCollectionName = 'outputCollection'
        DBObject command = new BasicDBObject('mapReduce', 'sourceCollection').append('out', outputCollectionName);

        when:
        MapReduce mapReduce = MapReduceCommand.getMapReduceFromDBObject(command)

        then:
        !mapReduce.inline
        mapReduce.getOutput().collectionName == outputCollectionName
    }

    //TODO: LOTS more tests around Output types in particular


}
