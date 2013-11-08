package org.mongodb.operation

import org.bson.types.Code
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoCursor
import org.mongodb.codecs.DocumentCodec
import org.mongodb.codecs.PrimitiveCodecs
import org.mongodb.command.MapReduceCommandResultCodec

import static org.hamcrest.CoreMatchers.not
import static org.hamcrest.Matchers.hasKey
import static org.mongodb.Fixture.bufferProvider
import static org.mongodb.Fixture.session
import static org.mongodb.ReadPreference.primary
import static spock.util.matcher.HamcrestSupport.that

class MapReduceOperationFunctionalSpecification extends FunctionalSpecification {

    def setup() {
        collection.save(['x': ['a', 'b'], 's': 1] as Document);
        collection.save(['x': ['b', 'c'], 's': 2] as Document);
        collection.save(['x': ['c', 'd'], 's': 3] as Document);
    }

    def 'when verbose is not set the command result should contain less information'() {
        given:
        MapReduce mapReduce = new MapReduce(new Code('function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }'),
                      new Code('function(key,values){ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}'))

        def codec = new MapReduceCommandResultCodec<Document>(PrimitiveCodecs.createDefault(), new DocumentCodec())
        MapReduceOperation operation = new MapReduceOperation<Document>(namespace, mapReduce, codec, primary(), bufferProvider,
                                                                        session, false, new DocumentCodec())


        when:
        MongoCursor<Document> results = operation.execute()

        then:
        that results.getCommandResult().getResponse(), not(hasKey('timing'))
    }

    def 'when verbose is set the command result should contain less information'() {
        given:
        MapReduce mapReduce = new MapReduce(new Code('function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }'),
                                            new Code('function(key,values)' +
                                                     '{ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}'))
        mapReduce.verbose();

        def codec = new MapReduceCommandResultCodec<Document>(PrimitiveCodecs.createDefault(), new DocumentCodec())
        MapReduceOperation operation = new MapReduceOperation<Document>(namespace, mapReduce, codec, primary(), bufferProvider,
                                                                        session, false, new DocumentCodec())


        when:
        MongoCursor<Document> results = operation.execute()

        then:
        that results.getCommandResult().getResponse(), hasKey('timing')
    }

}
