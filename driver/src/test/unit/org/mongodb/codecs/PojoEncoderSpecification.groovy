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
import org.mongodb.codecs.pojo.Address
import org.mongodb.codecs.pojo.Name
import org.mongodb.codecs.pojo.ObjectWithArray
import org.mongodb.codecs.pojo.ObjectWithMapOfStrings
import org.mongodb.codecs.pojo.Person
import spock.lang.Ignore
import spock.lang.Specification

import static org.junit.Assert.fail

class PojoEncoderSpecification extends Specification {
    private final BSONWriter bsonWriter = Mock();

    private final Codecs codecs = Codecs.createDefault();

    def shouldEncodeSimplePojo() {
        given:
        PojoEncoder<SimpleObject> pojoEncoder = new PojoEncoder<SimpleObject>(codecs);
        String valueInSimpleObject = 'MyName';

        when:
        pojoEncoder.encode(bsonWriter, new SimpleObject(valueInSimpleObject));

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('name');
        1 * bsonWriter.writeString(valueInSimpleObject);
        1 * bsonWriter.writeEndDocument();
    }

    def shouldEncodePojoContainingOtherPojos() {
        given:
        PojoEncoder<NestedObject> pojoEncoder = new PojoEncoder<NestedObject>(codecs);
        String anotherName = 'AnotherName';

        when:
        pojoEncoder.encode(bsonWriter, new NestedObject(new SimpleObject(anotherName)));

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('mySimpleObject');
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('name');
        1 * bsonWriter.writeString(anotherName);
        2 * bsonWriter.writeEndDocument();
    }

    def shouldEncodePojoContainingOtherPojosAndFields() {
        given:
        PojoEncoder<NestedObjectWithFields> pojoEncoder = new PojoEncoder<NestedObjectWithFields>(codecs);

        when:
        pojoEncoder.encode(bsonWriter, new NestedObjectWithFields(98, new SimpleObject('AnotherName')));

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('intValue');
        1 * bsonWriter.writeInt32(98);
        1 * bsonWriter.writeName('mySimpleObject');
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('name');
        1 * bsonWriter.writeString('AnotherName');
        2 * bsonWriter.writeEndDocument();
    }

    def shouldSupportArrays() {
        given:
        PojoEncoder<ObjectWithArray> pojoEncoder = new PojoEncoder<ObjectWithArray>(codecs);

        when:
        pojoEncoder.encode(bsonWriter, new ObjectWithArray());

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('theStringArray');
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeString('Uno');
        1 * bsonWriter.writeString('Dos');
        1 * bsonWriter.writeString('Tres');
        1 * bsonWriter.writeEndArray();
        1 * bsonWriter.writeEndDocument();
    }

    def shouldEncodeMapsOfPrimitiveTypes() {
        given:
        PojoEncoder<ObjectWithMapOfStrings> pojoEncoder = new PojoEncoder<ObjectWithMapOfStrings>(codecs);

        when:
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfStrings());

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('theMap');
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('first');
        1 * bsonWriter.writeString('the first value');
        1 * bsonWriter.writeName('second');
        1 * bsonWriter.writeString('the second value');
        2 * bsonWriter.writeEndDocument();
    }

    def shouldEncodeMapsOfObjects() {
        given:
        PojoEncoder<ObjectWithMapOfObjects> pojoEncoder = new PojoEncoder<ObjectWithMapOfObjects>(codecs);
        //TODO: get rid of this - default object codec is a bit of a smell
        codecs.setDefaultObjectCodec(new PojoCodec<ObjectWithMapOfObjects>(codecs, null));

        String simpleObjectValue = 'theValue';

        when:
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfObjects(simpleObjectValue));

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('theMap');
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('first');
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('name');
        1 * bsonWriter.writeString(simpleObjectValue);
        3 * bsonWriter.writeEndDocument();
    }

    def shouldEncodeMapsOfMaps() {
        given:
        PojoEncoder<ObjectWithMapOfMaps> pojoEncoder = new PojoEncoder<ObjectWithMapOfMaps>(codecs);

        when:
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfMaps());

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('theMap');
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('theMapInsideTheMap');
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('innerMapField');
        1 * bsonWriter.writeString('theInnerMapFieldValue');
        3 * bsonWriter.writeEndDocument();
    }

    def shouldNotEncodeSpecialFieldsLikeJacocoData() {
        given:
        PojoEncoder<JacocoDecoratedObject> pojoEncoder = new PojoEncoder<JacocoDecoratedObject>(codecs);
        JacocoDecoratedObject jacocoDecoratedObject = new JacocoDecoratedObject('thisName');

        when:
        pojoEncoder.encode(bsonWriter, jacocoDecoratedObject);

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeEndDocument();
    }

    def shouldEncodeComplexPojo() {
        given:
        PojoEncoder<Person> pojoEncoder = new PojoEncoder<Person>(codecs);
        Address address = new Address();
        Name name = new Name();
        Person person = new Person(address, name);

        when:
        pojoEncoder.encode(bsonWriter, person);

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('address');

        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('address1');
        1 * bsonWriter.writeString(address.getAddress1());
        1 * bsonWriter.writeName('address2');
        1 * bsonWriter.writeString(address.getAddress2());
        1 * bsonWriter.writeEndDocument();

        1 * bsonWriter.writeName('name');
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName('firstName');
        1 * bsonWriter.writeString(name.getFirstName());
        1 * bsonWriter.writeName('surname');
        1 * bsonWriter.writeString(name.getSurname());

        2 * bsonWriter.writeEndDocument();
    }

    def shouldIgnoreTransientFields() {
        given:
        PojoEncoder<ObjWithTransientField> pojoEncoder = new PojoEncoder<ObjWithTransientField>(codecs);
        String value = 'some value';

        when:
        pojoEncoder.encode(bsonWriter, new ObjWithTransientField(value));

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeEndDocument();

        0 * bsonWriter.writeName('transientField');
        0 * bsonWriter.writeString(value);
    }

    @Ignore('not implemented')
    def shouldEncodeIds() {
        given:
        fail('Not implemented');
    }

    @Ignore('not implemented')
    def shouldThrowAnExceptionWhenItCannotEncodeAField() {
        given:
        fail('Not implemented');
    }

    @Ignore('not implemented')
    def shouldEncodeEnumsAsStrings() {
        given:
        fail('Not implemented');
    }

    private static class JacocoDecoratedObject {
        @SuppressWarnings('FieldName')
        private final String $name;

        private JacocoDecoratedObject(String name) {
            this.$name = name;
        }
    }

    private static class SimpleObject {
        private final String name;

        private SimpleObject(String name) {
            this.name = name;
        }
    }

    private static class ObjWithTransientField {
        @SuppressWarnings('UnnecessaryTransientModifier')
        private final transient String transientField;

        private ObjWithTransientField(String transientField) {
            this.transientField = transientField;
        }
    }

    private static class NestedObject {
        private final SimpleObject mySimpleObject;

        private NestedObject(SimpleObject mySimpleObject) {
            this.mySimpleObject = mySimpleObject;
        }
    }

    private static class NestedObjectWithFields {
        private final int intValue;
        private final SimpleObject mySimpleObject;

        private NestedObjectWithFields(int intValue, SimpleObject mySimpleObject) {
            this.intValue = intValue;
            this.mySimpleObject = mySimpleObject;
        }
    }

    private static final class ObjectWithMapOfObjects {
        private final Map<String, SimpleObject> theMap = [:];

        private ObjectWithMapOfObjects(String theValue) {
            theMap.put('first', new SimpleObject(theValue));
        }
    }

    private static final class ObjectWithMapOfMaps {
        def theMap = ['theMapInsideTheMap': ['innerMapField': 'theInnerMapFieldValue']];
    }
}
