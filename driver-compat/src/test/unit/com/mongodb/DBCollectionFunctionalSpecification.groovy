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

package com.mongodb

class DBCollectionFunctionalSpecification extends FunctionalSpecification {
    private idOfExistingDocument

    def setupSpec() {
        Map.metaClass.bitwiseNegate = { new BasicDBObject(delegate) }
    }

    def setup() {
        def existingDocument = ~['a': ~[:],
                                 'b': ~[:]]
        collection.insert(existingDocument);
        idOfExistingDocument = existingDocument.get('_id')
        collection.setObjectClass(BasicDBObject)
    }

    def 'should drop collection that exists'() {
        given:
        collection.insert(~['name': 'myName']);

        when:
        collection.drop();

        then:
        !(this.collectionName in database.getCollectionNames())
    }

    def 'should not error when dropping a collection that does not exist'() {
        given:
        !(this.collectionName in database.getCollectionNames())

        when:
        collection.drop();

        then:
        notThrown(MongoException)
    }

    def 'should use top-level class for findAndModify'() {
        given:
        collection.setObjectClass(ClassA)

        when:
        DBObject document = collection.findAndModify(null, ~['_id': idOfExistingDocument,
                                                             'c'  : 1])

        then:
        document instanceof ClassA

    }

    def 'should use internal classes for findAndModify'() {
        given:
        collection.setInternalClass('a', ClassA);
        collection.setInternalClass('b', ClassB);

        when:
        DBObject document = collection.findAndModify(null, ~['_id': idOfExistingDocument,
                                                             'c'  : 1])

        then:
        document.get('a') instanceof ClassA
        document.get('b') instanceof ClassB
    }

    def 'should support index options'() {
        given:
        def options = ~[
                'sparse'            : true,
                'background'        : true,
                'expireAfterSeconds': 42,
                'somethingOdd'      : 'jeff'
        ]

        when:
        collection.ensureIndex(~['y': 1], options);

        then:
        collection.getIndexInfo().size() == 2

        DBObject document = collection.getIndexInfo()[1]
        document.get('expireAfterSeconds') == 42
        document.get('somethingOdd') == 'jeff'
        document.get('background') == true
        !document.containsField('dropDups')
    }

    def 'should should provided decoder factory for findAndModify'() {
        given:
        DBDecoder decoder = Mock()
        DBDecoderFactory factory = Mock()
        factory.create() >> decoder
        collection.setDBDecoderFactory(factory)

        when:
        collection.findAndModify(null, ~['_id': idOfExistingDocument,
                                         'c'  : 1])

        then:
        1 * decoder.decode(_ as byte[], collection)
    }

    def 'drop index should not fail if collection does not exist'() {
        given:
        collection.drop();

        expect:
        collection.dropIndex('indexOnCollectionThatDoesNotExist');
    }

    def 'drop index should error if index does not exist'() {
        given:
        collection.ensureIndex(new BasicDBObject('x', 1));

        when:
        collection.dropIndex('y_1');

        then:
        CommandFailureException exception = thrown(CommandFailureException)
        exception.getCommandResult().getErrorMessage() == 'index not found'
    }

    def 'should throw Exception if dropping an index with an incorrect type'() {
        given:
        BasicDBObject index = new BasicDBObject('x', 1);
        collection.ensureIndex(index);

        when:
        collection.dropIndex(new BasicDBObject('x', '2d'));

        then:
        CommandFailureException exception = thrown(CommandFailureException)
        exception.getCommandResult().getErrorMessage() == 'index not found'
    }

    def 'should drop nested index'() {
        given:
        collection.save(new BasicDBObject('x', new BasicDBObject('y', 1)));
        BasicDBObject index = new BasicDBObject('x.y', 1);
        collection.ensureIndex(index);
        assert collection.indexInfo.size() == 2

        when:
        collection.dropIndex(index);

        then:
        collection.indexInfo.size() == 1
    }

    def 'should drop all indexes except the default index on _id'() {
        given:
        collection.ensureIndex(new BasicDBObject('x', 1));
        collection.ensureIndex(new BasicDBObject('x.y', 1));
        assert collection.indexInfo.size() == 3

        when:
        collection.dropIndexes();

        then:
        collection.indexInfo.size() == 1
    }

    def 'should drop unique index'() {
        given:
        BasicDBObject index = new BasicDBObject('x', 1);
        collection.ensureIndex(index, new BasicDBObject('unique', true));

        when:
        collection.dropIndex(index);

        then:
        collection.indexInfo.size() == 1
    }

    static class ClassA extends BasicDBObject { }
    static class ClassB extends BasicDBObject { }

}
