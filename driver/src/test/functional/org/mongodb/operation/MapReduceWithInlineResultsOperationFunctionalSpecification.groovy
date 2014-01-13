/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.operation

import org.bson.types.Code
import org.mongodb.Document
import org.mongodb.FunctionalSpecification
import org.mongodb.MongoCursor
import org.mongodb.codecs.DocumentCodec
import org.mongodb.codecs.PrimitiveCodecs

import static org.hamcrest.CoreMatchers.not
import static org.hamcrest.Matchers.hasKey
import static org.mongodb.Fixture.bufferProvider
import static org.mongodb.Fixture.session
import static org.mongodb.ReadPreference.primary
import static spock.util.matcher.HamcrestSupport.that

class MapReduceWithInlineResultsOperationFunctionalSpecification extends FunctionalSpecification {

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
        MapReduceWithInlineResultsOperation operation = new MapReduceWithInlineResultsOperation(namespace, mapReduce, codec, primary(),
                                                                                                bufferProvider, session, false)

        when:
        MongoCursor<Document> results = operation.execute()

        then:
        that results.commandResult.getResponse(), not(hasKey('timing'))
    }

    def 'when verbose is set the command result should contain less information'() {
        given:
        MapReduce mapReduce = new MapReduce(new Code('function(){ for ( var i=0; i<this.x.length; i++ ){ emit( this.x[i] , 1 ); } }'),
                                            new Code('function(key,values)' +
                                                     '{ var sum=0; for( var i=0; i<values.length; i++ ) sum += values[i]; return sum;}'))
        mapReduce.verbose();

        def codec = new MapReduceCommandResultCodec<Document>(PrimitiveCodecs.createDefault(), new DocumentCodec())
        MapReduceWithInlineResultsOperation operation = new MapReduceWithInlineResultsOperation(namespace, mapReduce, codec, primary(),
                                                                                      bufferProvider, session, false)


        when:
        MongoCursor<Document> results = operation.execute()

        then:
        that results.commandResult.getResponse(), hasKey('timing')
    }

}
