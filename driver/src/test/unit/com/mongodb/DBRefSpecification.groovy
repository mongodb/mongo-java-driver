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

    def 'should be equal if two documents have same values whether or not they are the same objects'() {
        given:
        DBRef referenceA = new DBRef('foo.bar', 4);
        DBRef referenceB = new DBRef('foo.bar', 4);

        expect:
        referenceA == referenceB
        referenceA.hashCode() == referenceB.hashCode()
    }

    def 'should encode and decode DBRefs'() {
        given:
        DBRef reference = new DBRef('hello', 'world');
        DBObject document = new BasicDBObject('!', reference);
        OutputBuffer buffer = new BasicOutputBuffer();

        when:
        DefaultDBEncoder.FACTORY.create().writeObject(buffer, document);
        DefaultDBCallback callback = new DefaultDBCallback(null);
        BSONDecoder decoder = new BasicBSONDecoder();
        decoder.decode(buffer.toByteArray(), callback);

        then:
        '{ "!" : { "$ref" : "hello" , "$id" : "world"}}' == callback.get().toString()
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
