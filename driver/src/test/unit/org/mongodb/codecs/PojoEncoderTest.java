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

package org.mongodb.codecs;

import org.bson.BSONWriter;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.Synchroniser;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.codecs.pojo.Address;
import org.mongodb.codecs.pojo.Name;
import org.mongodb.codecs.pojo.Person;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class PojoEncoderTest {
    //CHECKSTYLE:OFF
    @Rule
    public final JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private BSONWriter bsonWriter;

    private final Codecs codecs = Codecs.createDefault();

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        context.setThreadingPolicy(new Synchroniser());
        bsonWriter = context.mock(BSONWriter.class);
    }

    @Test
    public void shouldEncodeSimplePojo() {
        final PojoEncoder<SimpleObject> pojoEncoder = new PojoEncoder<SimpleObject>(codecs);

        final String valueInSimpleObject = "MyName";
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeString(valueInSimpleObject);
            oneOf(bsonWriter).writeEndDocument();

        }});
        pojoEncoder.encode(bsonWriter, new SimpleObject(valueInSimpleObject));
    }

    @Test
    public void shouldEncodePojoContainingOtherPojos() {
        final PojoEncoder<NestedObject> pojoEncoder = new PojoEncoder<NestedObject>(codecs);

        final String anotherName = "AnotherName";
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("mySimpleObject");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeString(anotherName);
            exactly(2).of(bsonWriter).writeEndDocument();

        }});
        pojoEncoder.encode(bsonWriter, new NestedObject(new SimpleObject(anotherName)));
    }

    @Test
    public void shouldEncodePojoContainingOtherPojosAndFields() {
        final PojoEncoder<NestedObjectWithFields> pojoEncoder = new PojoEncoder<NestedObjectWithFields>(codecs);

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("intValue");
            oneOf(bsonWriter).writeInt32(98);
            oneOf(bsonWriter).writeName("mySimpleObject");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeString("AnotherName");
            exactly(2).of(bsonWriter).writeEndDocument();

        }});
        pojoEncoder.encode(bsonWriter, new NestedObjectWithFields(98, new SimpleObject("AnotherName")));
    }

    @Test
    public void shouldSupportArrays() {
        final PojoEncoder<ObjectWithArray> pojoEncoder = new PojoEncoder<ObjectWithArray>(codecs);

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("theStringArray");
            oneOf(bsonWriter).writeStartArray();
            oneOf(bsonWriter).writeString("Uno");
            oneOf(bsonWriter).writeString("Dos");
            oneOf(bsonWriter).writeString("Tres");
            oneOf(bsonWriter).writeEndArray();
            oneOf(bsonWriter).writeEndDocument();

        }});
        pojoEncoder.encode(bsonWriter, new ObjectWithArray());
    }

    @Test
    public void shouldEncodeMapsOfPrimitiveTypes() {
        final PojoEncoder<ObjectWithMapOfStrings> pojoEncoder = new PojoEncoder<ObjectWithMapOfStrings>(codecs);

        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("theMap");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("first");
            oneOf(bsonWriter).writeString("the first value");
            oneOf(bsonWriter).writeName("second");
            oneOf(bsonWriter).writeString("the second value");
            exactly(2).of(bsonWriter).writeEndDocument();

        }});
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfStrings());
    }

    @Test
    public void shouldEncodeMapsOfObjects() {
        final PojoEncoder<ObjectWithMapOfObjects> pojoEncoder = new PojoEncoder<ObjectWithMapOfObjects>(codecs);
        //TODO: get rid of this - default object codec is a bit of a smell
        codecs.setDefaultObjectCodec(new PojoCodec<ObjectWithMapOfObjects>(codecs, null));

        final String simpleObjectValue = "theValue";
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("theMap");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("first");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeString(simpleObjectValue);
            exactly(3).of(bsonWriter).writeEndDocument();

        }});
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfObjects(simpleObjectValue));
    }

    @Test
    public void shouldEncodeMapsOfMaps() {
        final PojoEncoder<ObjectWithMapOfMaps> pojoEncoder = new PojoEncoder<ObjectWithMapOfMaps>(codecs);
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("theMap");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("theMapInsideTheMap");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("innerMapField");
            oneOf(bsonWriter).writeString("theInnerMapFieldValue");
            exactly(3).of(bsonWriter).writeEndDocument();

        }});
        pojoEncoder.encode(bsonWriter, new ObjectWithMapOfMaps());
    }

    @Test
    public void shouldNotEncodeSpecialFieldsLikeJacocoData() {
        final PojoEncoder<JacocoDecoratedObject> pojoEncoder = new PojoEncoder<JacocoDecoratedObject>(codecs);
        final JacocoDecoratedObject jacocoDecoratedObject = new JacocoDecoratedObject("thisName");
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeEndDocument();

        }});

        pojoEncoder.encode(bsonWriter, jacocoDecoratedObject);
    }

    @Test
    public void shouldEncodeComplexPojo() {
        final PojoEncoder<Person> pojoEncoder = new PojoEncoder<Person>(codecs);
        final Address address = new Address();
        final Name name = new Name();
        final Person person = new Person(address, name);
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("address");

            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("address1");
            oneOf(bsonWriter).writeString(address.getAddress1());
            oneOf(bsonWriter).writeName("address2");
            oneOf(bsonWriter).writeString(address.getAddress2());
            oneOf(bsonWriter).writeEndDocument();

            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("firstName");
            oneOf(bsonWriter).writeString(name.getFirstName());
            oneOf(bsonWriter).writeName("surname");
            oneOf(bsonWriter).writeString(name.getSurname());

            exactly(2).of(bsonWriter).writeEndDocument();

        }});

        pojoEncoder.encode(bsonWriter, person);
    }

    @Test
    public void shouldIgnoreTransientFields() {
        final PojoEncoder<ObjWithTransientField> pojoEncoder = new PojoEncoder<ObjWithTransientField>(codecs);

        final String value = "some value";
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeEndDocument();

            never(bsonWriter).writeName("transientField");
            never(bsonWriter).writeString(value);

        }});

        pojoEncoder.encode(bsonWriter, new ObjWithTransientField(value));
    }

    @Test
    @Ignore("not implemented")
    public void shouldEncodeIds() {
        fail("Not implemented");
    }

    @Test
    @Ignore("not implemented")
    public void shouldThrowAnExceptionWhenItCannotEncodeAField() {
        fail("Not implemented");
    }

    @Test
    @Ignore("not implemented")
    public void shouldEncodeEnumsAsStrings() {
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

    private static final class ObjectWithArray {
        private final String[] theStringArray = {"Uno", "Dos", "Tres"};
    }

    private static final class ObjectWithMapOfStrings {
        private final Map<String, String> theMap = new HashMap<String, String>();

        {
            theMap.put("first", "the first value");
            theMap.put("second", "the second value");
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
