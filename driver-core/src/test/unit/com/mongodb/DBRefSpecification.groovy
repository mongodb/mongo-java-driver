/*
 * Copyright 2008-2017 MongoDB, Inc.
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

class DBRefSpecification extends Specification {

    def 'should set properties'() {
        when:
        DBRef referenceA = new DBRef('foo.bar', 5)
        DBRef referenceB = new DBRef('mydb', 'foo.bar', 5)
        DBRef referenceC = new DBRef(null, 'foo.bar', 5)

        then:
        referenceA.databaseName == null
        referenceA.collectionName == 'foo.bar'
        referenceA.id == 5
        referenceB.databaseName == 'mydb'
        referenceB.collectionName == 'foo.bar'
        referenceB.id == 5
        referenceC.databaseName == null
        referenceC.collectionName == 'foo.bar'
        referenceC.id == 5
    }

    def 'constructor should throw if collection name is null'() {
        when:
        new DBRef(null, 5)

        then:
        thrown(IllegalArgumentException)
    }

    def 'constructor should throw if id is null'() {
        when:
        new DBRef('foo.bar', null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'equivalent instances should be equal and have the same hash code'() {
        given:
        DBRef referenceA = new DBRef('foo.bar', 4)
        DBRef referenceB = new DBRef('foo.bar', 4)
        DBRef referenceC = new DBRef('mydb', 'foo.bar', 4)
        DBRef referenceD = new DBRef('mydb', 'foo.bar', 4)

        expect:
        referenceA.equals(referenceA)
        referenceA.equals(referenceB)
        referenceC.equals(referenceD)
        referenceA.hashCode() == referenceB.hashCode()
        referenceC.hashCode() == referenceD.hashCode()
    }

    def 'non-equivalent instances should not be equal and have different hash codes'() {
        given:
        DBRef referenceA = new DBRef('foo.bar', 4)
        DBRef referenceB = new DBRef('foo.baz', 4)
        DBRef referenceC = new DBRef('foo.bar', 5)
        DBRef referenceD = new DBRef('mydb', 'foo.bar', 4)
        DBRef referenceE = new DBRef('yourdb', 'foo.bar', 4)

        expect:
        !referenceA.equals(null)
        !referenceA.equals('some other class instance')
        !referenceA.equals(referenceB)
        referenceA.hashCode() != referenceB.hashCode()
        !referenceA.equals(referenceC)
        referenceA.hashCode() != referenceC.hashCode()
        !referenceA.equals(referenceD)
        referenceA.hashCode() != referenceD.hashCode()
        !referenceD.equals(referenceE)
        referenceD.hashCode() != referenceE.hashCode()
    }

    def 'should stringify'() {
        expect:
        new DBRef('foo.bar', 4).toString() == '{ "$ref" : "foo.bar", "$id" : "4" }'
        new DBRef('mydb', 'foo.bar', 4).toString() == '{ "$ref" : "foo.bar", "$id" : "4", "$db" : "mydb" }'
    }

    def 'testSerialization'() throws Exception {
        given:
        DBRef originalDBRef = new DBRef('col', 42)

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)

        when:
        objectOutputStream.writeObject(originalDBRef)
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(outputStream.toByteArray()))
        DBRef deserializedDBRef = (DBRef) objectInputStream.readObject()

        then:
        originalDBRef == deserializedDBRef
    }
}
