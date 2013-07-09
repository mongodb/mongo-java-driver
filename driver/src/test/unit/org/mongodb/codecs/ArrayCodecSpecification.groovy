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

package org.mongodb.codecs

import org.bson.BSONWriter
import org.bson.types.Binary
import spock.lang.Specification
import spock.lang.Subject

class ArrayCodecSpecification extends Specification {
    private final BSONWriter bsonWriter = Mock();

    @Subject
    private final ArrayCodec arrayCodec  = new ArrayCodec(null);

    def 'should encode array of ints'() {
        given:
        int[] arrayOfInts = [1, 2, 3];

        when:
        arrayCodec.encode(bsonWriter, arrayOfInts);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeInt32(1);
        1 * bsonWriter.writeInt32(2);
        1 * bsonWriter.writeInt32(3);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of ints when it is disguised as an object'() {
        given:
        Object arrayOfInts = [1, 2, 3] as int[];

        when:
        arrayCodec.encode(bsonWriter, arrayOfInts);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeInt32(1);
        1 * bsonWriter.writeInt32(2);
        1 * bsonWriter.writeInt32(3);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of longs'() {
        given:
        long[] array = [1, 2, 3];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeInt64(1);
        1 * bsonWriter.writeInt64(2);
        1 * bsonWriter.writeInt64(3);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of longs when it is disguised as an object'() {
        given:
        Object array = [1, 2, 3] as long[];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeInt64(1);
        1 * bsonWriter.writeInt64(2);
        1 * bsonWriter.writeInt64(3);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of boolean'() {
        given:
        boolean[] array = [true, false];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeBoolean(true);
        1 * bsonWriter.writeBoolean(false);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of boolean when it is disguised as an object'() {
        given:
        Object array = [true, false] as boolean[];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeBoolean(true);
        1 * bsonWriter.writeBoolean(false);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of byte'() {
        given:
        byte[] array = [1, 2];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeBinaryData(new Binary(array));
    }

    def 'should encode array of byte when it is disguised as an object'() {
        given:
        byte[] byteArray = [1, 2];

        when:
        arrayCodec.encode(bsonWriter, (Object)byteArray);

        then:
        1 * bsonWriter.writeBinaryData(new Binary(byteArray));
    }

    def 'should encode array of double'() {
        when:
        arrayCodec.encode(bsonWriter, [1.1, 2.2] as double[]);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeDouble(1.1);
        1 * bsonWriter.writeDouble(2.2);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of double when it is disguised as an object'() {
        given:
        Object array = [1.1, 2.2] as double[];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeDouble(1.1);
        1 * bsonWriter.writeDouble(2.2);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of float'() {
        given:
        float[] array = [1.4F, 2.6F];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeDouble(1.4F);
        1 * bsonWriter.writeDouble(2.6F);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of float when it is disguised as an object'() {
        given:
        Object array = [1.4F, 2.6F] as float[];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeDouble(1.4F);
        1 * bsonWriter.writeDouble(2.6F);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of short'() {
        given:
        short[] array = [3, 4];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeInt32(3);
        1 * bsonWriter.writeInt32(4);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of short when it is disguised as an object'() {
        given:
        Object array = [3, 4] as short[];

        when:
        arrayCodec.encode(bsonWriter, array);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeInt32(3);
        1 * bsonWriter.writeInt32(4);
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of strings'() {
        given:
        String[] arrayOfStrings = ['1', '2', '3'];

        when:
        arrayCodec.encode(bsonWriter, arrayOfStrings);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeString('1');
        1 * bsonWriter.writeString('2');
        1 * bsonWriter.writeString('3');
        1 * bsonWriter.writeEndArray();
    }

    def 'should encode array of strings when it is disguised as an object'() {
        given:
        Object arrayOfStrings = ['1', '2', '3'] as String[];

        when:
        arrayCodec.encode(bsonWriter, arrayOfStrings);

        then:
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeString('1');
        1 * bsonWriter.writeString('2');
        1 * bsonWriter.writeString('3');
        1 * bsonWriter.writeEndArray();
    }

}
