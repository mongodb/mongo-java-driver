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

import org.bson.BSONDecoder
import org.bson.BasicBSONDecoder
import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
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
        DBRef referenceA = new DBRef('foo.bar', 4);
        DBRef referenceB = new DBRef('foo.bar', 4);
        DBRef referenceC = new DBRef('mydb', 'foo.bar', 4);
        DBRef referenceD = new DBRef('mydb', 'foo.bar', 4);

        expect:
        referenceA.equals(referenceA)
        referenceA.equals(referenceB)
        referenceC.equals(referenceD)
        referenceA.hashCode() == referenceB.hashCode()
        referenceC.hashCode() == referenceD.hashCode()
    }

    def 'non-equivalent instances should not be equal and have different hash codes'() {
        given:
        DBRef referenceA = new DBRef('foo.bar', 4);
        DBRef referenceB = new DBRef('foo.baz', 4);
        DBRef referenceC = new DBRef('foo.bar', 5);
        DBRef referenceD = new DBRef('mydb', 'foo.bar', 4);
        DBRef referenceE = new DBRef('yourdb', 'foo.bar', 4);

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
        new DBRef('foo.bar', 4).toString() == '{ "$ref" : "foo.bar", "$id" : "4 }'
        new DBRef('mydb', 'foo.bar', 4).toString() == '{ "$ref" : "foo.bar", "$id" : "4, "$db" : "mydb" }'
    }

    def 'should encode and decode DBRefs'() {
        given:
        DBRef reference = new DBRef('coll', 'hello world');
        DBObject document = new BasicDBObject('!', reference);
        OutputBuffer buffer = new BasicOutputBuffer();

        when:
        DefaultDBEncoder.FACTORY.create().writeObject(buffer, document);
        DefaultDBCallback callback = new DefaultDBCallback(null);
        BSONDecoder decoder = new BasicBSONDecoder();
        decoder.decode(buffer.toByteArray(), callback);
        DBRef decoded = ((DBObject) callback.get()).get('!');

        then:
        decoded.databaseName == null
        decoded.collectionName == 'coll'
        decoded.id == 'hello world'
    }

    def 'should encode and decode DBRefs with a database name'() {
        given:
        DBRef reference = new DBRef('db', 'coll', 'hello world');
        DBObject document = new BasicDBObject('!', reference);
        OutputBuffer buffer = new BasicOutputBuffer();

        when:
        DefaultDBEncoder.FACTORY.create().writeObject(buffer, document);
        DefaultDBCallback callback = new DefaultDBCallback(null);
        BSONDecoder decoder = new BasicBSONDecoder();
        decoder.decode(buffer.toByteArray(), callback);
        DBRef decoded = ((DBObject) callback.get()).get('!');

        then:
        decoded.databaseName == 'db'
        decoded.collectionName == 'coll'
        decoded.id == 'hello world'
    }

    def 'testSerialization'() throws Exception {
        given:
        DBRef originalDBRef = new DBRef('col', 42);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

        when:
        objectOutputStream.writeObject(originalDBRef);
        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(outputStream.toByteArray()));
        DBRef deserializedDBRef = (DBRef) objectInputStream.readObject();

        then:
        originalDBRef == deserializedDBRef
    }
}
