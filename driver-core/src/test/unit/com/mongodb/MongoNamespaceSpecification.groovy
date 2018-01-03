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

import spock.lang.Specification

class MongoNamespaceSpecification extends Specification {
    def 'invalid database name should throw IllegalArgumentException'() {
        when:
        new MongoNamespace(databaseName, 'test');

        then:
        thrown(IllegalArgumentException)

        when:
        MongoNamespace.checkDatabaseNameValidity(databaseName)

        then:
        thrown(IllegalArgumentException)

        where:
        databaseName << [null, '', 'a\0b', 'a b', 'a.b', 'a/b', 'a\\b', 'a"b']
    }

    def 'invalid collection name should throw IllegalArgumentException'() {
        when:
        new MongoNamespace('test', collectionName);

        then:
        thrown(IllegalArgumentException)

        when:
        MongoNamespace.checkCollectionNameValidity(collectionName)

        then:
        thrown(IllegalArgumentException)

        where:
        collectionName << [null, '']
    }

    def 'invalid full name should throw IllegalArgumentException'() {
        when:
        new MongoNamespace(fullName)

        then:
        thrown(IllegalArgumentException)

        where:
        fullName << [null, '', 'db', '.db', 'db.', 'a .b']
    }

    def 'test getters'() {
        expect:
        namespace.getDatabaseName() == 'db'
        namespace.getCollectionName() == 'a.b'
        namespace.getFullName() == 'db.a.b'

        where:
        namespace << [new MongoNamespace('db', 'a.b'), new MongoNamespace('db.a.b')]
    }

    @SuppressWarnings('ComparisonWithSelf')
    def 'testEqualsAndHashCode'() {
        given:
        MongoNamespace namespace1 = new MongoNamespace('db1', 'coll1');
        MongoNamespace namespace2 = new MongoNamespace('db1', 'coll1');
        MongoNamespace namespace3 = new MongoNamespace('db2', 'coll1');
        MongoNamespace namespace4 = new MongoNamespace('db1', 'coll2');

        expect:
        namespace1 != new Object()
        namespace1 == namespace1;
        namespace1 == namespace2;
        namespace1 != namespace3;
        namespace1 != namespace4;

        namespace1.hashCode() == 97917362
    }
}
