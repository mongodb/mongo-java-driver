package org.mongodb.command

import org.mongodb.Document
import org.mongodb.connection.ServerAddress
import org.mongodb.operation.CommandResult
import spock.lang.Specification
import spock.lang.Subject

class MapReduceCommandResultSpecification extends Specification {

    def setupSpec() {
        Map.metaClass.asType = { Class type ->
            if (type == Document) {
                return new Document(delegate)
            }
        }
    }

    @Subject
    private MapReduceCommandResult<Document> commandResult

    def 'should extract correct value from "results" field'() {
        when:
        commandResult = new MapReduceCommandResult<>(new CommandResult(
                [:] as Document,
                new ServerAddress(),
                ['results': [new Document('a', 1), new Document('b', 2)]] as Document,
                0
        ));

        then:
        commandResult.isInline()
        commandResult.getValue() instanceof Iterable<Document>
    }

    def 'should extract collectionName from result'() {
        when:
        commandResult = new MapReduceCommandResult<>(new CommandResult(
                [:] as Document,
                new ServerAddress(),
                ['result': 'foo'] as Document,
                0
        ));

        then:
        commandResult.getTargetCollectionName() == 'foo'
    }

    def 'should extract databaseName and collectionName from result'() {
        when:
        commandResult = new MapReduceCommandResult<>(new CommandResult(
                [:] as Document,
                new ServerAddress(),
                ['result': ['collection': 'foo', 'db': 'bar'] as Document] as Document,
                0
        ));

        then:
        commandResult.getTargetCollectionName() == 'foo'
        commandResult.getTargetDatabaseName() == 'bar'
    }

    def 'should return null if there is no database name in result'() {
        when:
        commandResult = new MapReduceCommandResult<>(new CommandResult(
                [:] as Document,
                new ServerAddress(),
                ['result': ['collection': 'foo'] as Document] as Document,
                0
        ));

        then:
        commandResult.getTargetDatabaseName() == null
    }

    def 'should throw on getting collectionName for result with inlined data'(){
        setup:
        commandResult = new MapReduceCommandResult<>(new CommandResult(
                [:] as Document,
                new ServerAddress(),
                ['results': [new Document('a', 1), new Document('b', 2)]] as Document,
                0
        ));

        when:
        commandResult.getTargetCollectionName()

        then:
        thrown(IllegalAccessError)
    }

}
