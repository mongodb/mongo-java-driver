/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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





package org.mongodb.command

import org.mongodb.CommandResult
import org.mongodb.Document
import org.mongodb.connection.ServerAddress
import spock.lang.Specification
import spock.lang.Subject

class MapReduceCommandResultsSpecification extends Specification {

    def setupSpec() {
        Map.metaClass.asType = { Class type ->
            if (type == Document) {
                return new Document(delegate)
            }
        }
    }

    @Subject
    private CommandResult commandResult

    def 'should extract correct value from "results" field'() {
        when:
        commandResult = new MapReduceInlineCommandResult<>(new CommandResult(
                new ServerAddress(),
                ['results': [new Document('a', 1), new Document('b', 2)]] as Document,
                0
        ));

        then:
        commandResult.getResults() instanceof Iterable<Document>
    }

    def 'should extract collectionName from result'() {
        when:
        commandResult = new MapReduceCommandResult<>(new CommandResult(
                new ServerAddress(),
                ['result': 'foo'] as Document,
                0
        ));

        then:
        commandResult.getCollectionName() == 'foo'
    }

    def 'should extract databaseName and collectionName from result'() {
        when:
        commandResult = new MapReduceCommandResult<>(new CommandResult(
                new ServerAddress(),
                ['result': ['collection': 'foo', 'db': 'bar'] as Document] as Document,
                0
        ));

        then:
        commandResult.getCollectionName() == 'foo'
        commandResult.getDatabaseName() == 'bar'
    }

    def 'should return null if there is no database name in result'() {
        when:
        commandResult = new MapReduceCommandResult<>(new CommandResult(
                new ServerAddress(),
                ['result': ['collection': 'foo'] as Document] as Document,
                0
        ));

        then:
        commandResult.getDatabaseName() == null
    }

}
