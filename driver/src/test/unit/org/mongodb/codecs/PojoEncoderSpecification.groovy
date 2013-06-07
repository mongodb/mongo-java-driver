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
    private BSONWriter bsonWriter = Mock();

    private final Codecs codecs = Codecs.createDefault();

    public void shouldEncodeSimplePojo() {
        setup:
        final PojoEncoder<SimpleObject> pojoEncoder = new PojoEncoder<SimpleObject>(codecs);
        final String valueInSimpleObject = "MyName";
        
        when:
        pojoEncoder.encode(bsonWriter, new SimpleObject(valueInSimpleObject));
        
        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("name");
        1 * bsonWriter.writeString(valueInSimpleObject);
        1 * bsonWriter.writeEndDocument();
    }

    public void shouldEncodePojoContainingOtherPojos() {
        setup:
        final PojoEncoder<NestedObject> pojoEncoder = new PojoEncoder<NestedObject>(codecs);
        final String anotherName = "AnotherName";

        when:
        pojoEncoder.encode(bsonWriter, new NestedObject(new SimpleObject(anotherName)));
        
        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("mySimpleObject");
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("name");
        1 * bsonWriter.writeString(anotherName);
        2 * bsonWriter.writeEndDocument();
    }

    public void shouldEncodePojoContainingOtherPojosAndFields() {
        setup:
        final PojoEncoder<NestedObjectWithFields> pojoEncoder = new PojoEncoder<NestedObjectWithFields>(codecs);

        when:
        pojoEncoder.encode(bsonWriter, new NestedObjectWithFields(98, new SimpleObject("AnotherName")));
        
        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("intValue");
        1 * bsonWriter.writeInt32(98);
        1 * bsonWriter.writeName("mySimpleObject");
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("name");
        1 * bsonWriter.writeString("AnotherName");
        2 * bsonWriter.writeEndDocument();
    }

    public void shouldSupportArrays() {
        setup:
        final PojoEncoder<ObjectWithArray> pojoEncoder = new PojoEncoder<ObjectWithArray>(codecs);

        when:
        pojoEncoder.encode(bsonWriter, new ObjectWithArray());

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("theStringArray");
        1 * bsonWriter.writeStartArray();
        1 * bsonWriter.writeString("Uno");
        1 * bsonWriter.writeString("Dos");
        1 * bsonWriter.writeString("Tres");
        1 * bsonWriter.writeEndArray();
        1 * bsonWriter.writeEndDocument();
    }

    public void shouldEncodeMapsOfPrimitiveTypes() {
        setup:
        final PojoEncoder<ObjectWithMapOfStrings> pojoEncoder = new PojoEncoder<ObjectWithMapOfStrings>(codecs);

        when:
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfStrings());

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("theMap");
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("first");
        1 * bsonWriter.writeString("the first value");
        1 * bsonWriter.writeName("second");
        1 * bsonWriter.writeString("the second value");
        2 * bsonWriter.writeEndDocument();
    }

    public void shouldEncodeMapsOfObjects() {
        setup:
        final PojoEncoder<ObjectWithMapOfObjects> pojoEncoder = new PojoEncoder<ObjectWithMapOfObjects>(codecs);
        //TODO: get rid of this - default object codec is a bit of a smell
        codecs.setDefaultObjectCodec(new PojoCodec<ObjectWithMapOfObjects>(codecs, null));

        final String simpleObjectValue = "theValue";
        
        when:
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfObjects(simpleObjectValue));

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("theMap");
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("first");
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("name");
        1 * bsonWriter.writeString(simpleObjectValue);
        3 * bsonWriter.writeEndDocument();
    }

    public void shouldEncodeMapsOfMaps() {
        setup:
        final PojoEncoder<ObjectWithMapOfMaps> pojoEncoder = new PojoEncoder<ObjectWithMapOfMaps>(codecs);

        when:
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfMaps());

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("theMap");
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("theMapInsideTheMap");
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("innerMapField");
        1 * bsonWriter.writeString("theInnerMapFieldValue");
        3 * bsonWriter.writeEndDocument();
    }

    public void shouldNotEncodeSpecialFieldsLikeJacocoData() {
        setup:
        final PojoEncoder<JacocoDecoratedObject> pojoEncoder = new PojoEncoder<JacocoDecoratedObject>(codecs);
        final JacocoDecoratedObject jacocoDecoratedObject = new JacocoDecoratedObject("thisName");

        when:
        pojoEncoder.encode(bsonWriter, jacocoDecoratedObject);

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeEndDocument();
    }

    public void shouldEncodeComplexPojo() {
        setup:
        final PojoEncoder<Person> pojoEncoder = new PojoEncoder<Person>(codecs);
        final Address address = new Address();
        final Name name = new Name();
        final Person person = new Person(address, name);

        when:
        pojoEncoder.encode(bsonWriter, person);

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("address");

        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("address1");
        1 * bsonWriter.writeString(address.getAddress1());
        1 * bsonWriter.writeName("address2");
        1 * bsonWriter.writeString(address.getAddress2());
        1 * bsonWriter.writeEndDocument();

        1 * bsonWriter.writeName("name");
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeName("firstName");
        1 * bsonWriter.writeString(name.getFirstName());
        1 * bsonWriter.writeName("surname");
        1 * bsonWriter.writeString(name.getSurname());

        2 * bsonWriter.writeEndDocument();
    }

    public void shouldIgnoreTransientFields() {
        setup:
        final PojoEncoder<ObjWithTransientField> pojoEncoder = new PojoEncoder<ObjWithTransientField>(codecs);
        final String value = "some value";

        when:
        pojoEncoder.encode(bsonWriter, new ObjWithTransientField(value));

        then:
        1 * bsonWriter.writeStartDocument();
        1 * bsonWriter.writeEndDocument();

        0 * bsonWriter.writeName("transientField");
        0 * bsonWriter.writeString(value);
    }

    @Ignore("not implemented")
    public void shouldEncodeIds() {
        setup:
        fail("Not implemented");
    }

    @Ignore("not implemented")
    public void shouldThrowAnExceptionWhenItCannotEncodeAField() {
        setup:
        fail("Not implemented");
    }

    @Ignore("not implemented")
    public void shouldEncodeEnumsAsStrings() {
        setup:
        fail("Not implemented");
    }

    //CHECKSTYLE:OFF
    private static class JacocoDecoratedObject {
        private final String $name;

        public JacocoDecoratedObject(final String name) {
            this.$name = name;
        }
    }
    //CHECKSTYLE:ON

    private static class SimpleObject {
        private final String name;

        public SimpleObject(final String name) {
            this.name = name;
        }
    }

    private static class ObjWithTransientField {
        private final transient String transientField;

        public ObjWithTransientField(final String transientField) {
            this.transientField = transientField;
        }
    }

    private static class NestedObject {
        private final SimpleObject mySimpleObject;

        public NestedObject(final SimpleObject mySimpleObject) {
            this.mySimpleObject = mySimpleObject;
        }
    }

    private static class NestedObjectWithFields {
        private final int intValue;
        private final SimpleObject mySimpleObject;

        public NestedObjectWithFields(final int intValue, final SimpleObject mySimpleObject) {
            this.intValue = intValue;
            this.mySimpleObject = mySimpleObject;
        }
    }

    private static final class ObjectWithMapOfObjects {
        private final Map<String, SimpleObject> theMap = new HashMap<String, SimpleObject>();

        private ObjectWithMapOfObjects(final String theValue) {
            theMap.put("first", new SimpleObject(theValue));
        }
    }

    private static final class ObjectWithMapOfMaps {
        private final Map<String, Map<String, String>> theMap = new HashMap<String, Map<String, String>>();

        private ObjectWithMapOfMaps() {
            final Map<String, String> innerMap = new HashMap<String, String>();
            innerMap.put("innerMapField", "theInnerMapFieldValue");
            theMap.put("theMapInsideTheMap", innerMap);
        }
    }
}
