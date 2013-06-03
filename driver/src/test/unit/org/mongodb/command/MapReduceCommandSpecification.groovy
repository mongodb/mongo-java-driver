package org.mongodb.command

import org.bson.types.Code
import org.mongodb.Document
import org.mongodb.operation.MapReduce as MR
import org.mongodb.operation.MapReduceOutput
import spock.lang.Specification

class MapReduceCommandSpecification extends Specification {

    def setupSpec() {
        Map.metaClass.asType = { Class type ->
            if (type == Document) {
                return new Document(delegate)
            }
        }
    }


    @SuppressWarnings('DuplicateMapLiteral')
    def 'should convert into correct documents'() {
        expect:
        Document document = new MapReduce(mapReduce, 'foo').toDocument();
        document.get('query') == query
        document.get('sort') == sort
        document.get('limit') == limit
        document.get('finalize') == finalize
        document.get('scope') == scope
        document.get('jsMode') == jsMode
        document.get('verbose') == verbose

        where:
        mapReduce                                  | query                | sort | limit | finalize      | scope          | jsMode | verbose
        new MR(new Code('a'), new Code('b'))       | null                 | null | 0     | null          | null           | false  | false
        new MR(new Code('a'), new Code('b'))
                .filter(['c': 1] as Document)      | new Document('c', 1) | null | 0     | null          | null           | false  | false
        new MR(new Code('a'), new Code('b'))
                .finalize(new Code('c'))           | null                 | null | 0     | new Code('c') | null           | false  | false
        new MR(new Code('a'), new Code('b'))
                .jsMode()                          | null                 | null | 0     | null          | null           | true   | false
        new MR(new Code('a'), new Code('b'))
                .verbose()                         | null                 | null | 0     | null          | null           | false  | true
        new MR(new Code('a'), new Code('b'))
                .scope([p: 2, q: 's'] as Document) | null                 | null | 0     | null          | [p: 2, q: 's'] | false  | false
        new MR(new Code('a'), new Code('b'))
                .limit(10)                         | null                 | null | 10    | null          | null           | false  | false
    }

    @SuppressWarnings('DuplicateMapLiteral')
    def 'should convert output into correct document'() {
        expect:
        new MapReduce(mapReduce, 'foo').toDocument().get('out') == document

        where:
        output << [
                new MapReduceOutput('foo'),
                new MapReduceOutput('foo').database('bar'),
                new MapReduceOutput('foo').action(MapReduceOutput.Action.MERGE),
                new MapReduceOutput('foo').action(MapReduceOutput.Action.REPLACE),
                new MapReduceOutput('foo').database('bar').action(MapReduceOutput.Action.REDUCE),
                new MapReduceOutput('foo').sharded(),
                new MapReduceOutput('foo').nonAtomic(),
                new MapReduceOutput('foo').database('bar').sharded()
        ]
        mapReduce = new MR(new Code('a'), new Code('b'), output)
        document << [
                ['replace': 'foo', 'sharded': false, 'nonAtomic': false] as Document,
                ['replace': 'foo', 'db': 'bar', 'sharded': false, 'nonAtomic': false] as Document,
                ['merge': 'foo', 'sharded': false, 'nonAtomic': false] as Document,
                ['replace': 'foo', 'sharded': false, 'nonAtomic': false] as Document,
                ['reduce': 'foo', 'db': 'bar', 'sharded': false, 'nonAtomic': false] as Document,
                ['replace': 'foo', 'sharded': true, 'nonAtomic': false] as Document,
                ['replace': 'foo', 'sharded': false, 'nonAtomic': true] as Document,
                ['replace': 'foo', 'db': 'bar', 'sharded': true, 'nonAtomic': false] as Document,
        ]
    }


}
