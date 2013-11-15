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

package org.mongodb.operation

import org.bson.types.Code
import org.mongodb.Document
import org.mongodb.operation.MapReduce as MR
import spock.lang.Specification

class CommandDocumentsHelperSpecification extends Specification {

    @SuppressWarnings('DuplicateMapLiteral')
    def 'should convert into correct documents'() {
        expect:
        Document document = CommandDocuments.createMapReduce('foo', mapReduce);
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
        CommandDocuments.createMapReduce('foo', mapReduce).get('out') == document

        where:
        output << [
                new MapReduceOutputOptions('foo'),
                new MapReduceOutputOptions('foo').database('bar'),
                new MapReduceOutputOptions('foo').action(MapReduceOutputOptions.Action.MERGE),
                new MapReduceOutputOptions('foo').action(MapReduceOutputOptions.Action.REPLACE),
                new MapReduceOutputOptions('foo').database('bar').action(MapReduceOutputOptions.Action.REDUCE),
                new MapReduceOutputOptions('foo').sharded(),
                new MapReduceOutputOptions('foo').nonAtomic(),
                new MapReduceOutputOptions('foo').database('bar').sharded()
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
