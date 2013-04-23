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
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mongodb.json.JSONWriter;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class PojoCodecTest {
    //CHECKSTYLE:OFF
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private BSONWriter bsonWriter;
    private PojoCodec<Object> pojoCodec;
    private Codecs codecs = Codecs.createDefault();

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        bsonWriter = context.mock(BSONWriter.class);
        pojoCodec = new PojoCodec<Object>(codecs);
    }

    @Test
    public void shouldEncodeSimplePojo() {
        final String valueInSimpleObject = "MyName";
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeString(valueInSimpleObject);
            oneOf(bsonWriter).writeEndDocument();

        }});
        ignoreJacocoInvocations(bsonWriter);

        pojoCodec.encode(bsonWriter, new SimpleObject(valueInSimpleObject));
    }

    @Test
    public void shouldEncodePojoContainingOtherPojos() {
        final String anotherName = "AnotherName";
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("mySimpleObject");
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeString(anotherName);
            exactly(2).of(bsonWriter).writeEndDocument();

        }});
        ignoreJacocoInvocations(bsonWriter);

        pojoCodec.encode(bsonWriter, new NestedObject(new SimpleObject(anotherName)));
    }

    @Test
    public void shouldEncodePojoContainingOtherPojosAndFields() {
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
        ignoreJacocoInvocations(bsonWriter);

        pojoCodec.encode(bsonWriter, new NestedObjectWithFields(98, new SimpleObject("AnotherName")));
    }

    @Test
    @Ignore("should be able to use the JSON form to check the object")
    public void shouldEncodeSimplePojo2() {
        final StringWriter writer = new StringWriter();
        pojoCodec.encode(new JSONWriter(writer), new SimpleObject("MyName"));

        System.out.println(writer.toString());
    }

    @Test
    @Ignore("not implemented")
    public void shouldEncodePojoContainingOtherPojos2() {
        final StringWriter writer = new StringWriter();
        pojoCodec.encode(new JSONWriter(writer), new NestedObject(new SimpleObject("AnotherName")));
        System.out.println(writer.toString());
    }

    @Test
    public void shouldSupportArrays() {
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
        ignoreJacocoInvocations(bsonWriter);

        pojoCodec.encode(bsonWriter, new ObjectWithArray());
    }

    @Test
    public void shouldEncodeMapsOfPrimitiveTypes() {
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
        ignoreJacocoInvocations(bsonWriter);
        pojoCodec.encode(bsonWriter, new ObjectWithMapOfStrings());
    }

    @Test
    public void shouldEncodeMapsOfObjects() {
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
        ignoreJacocoInvocations(bsonWriter);

        pojoCodec.encode(bsonWriter, new ObjectWithMapOfObjects(simpleObjectValue));
    }

    @Test
    public void shouldEncodeMapsOfMaps() {
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
        ignoreJacocoInvocations(bsonWriter);

        pojoCodec.encode(bsonWriter, new ObjectWithMapOfMaps());
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

    private void ignoreJacocoInvocations(final BSONWriter writer) {
        //URGH
        context.checking(new Expectations() {{
            ignoring(writer);
        }});
    }

    private static class SimpleObject {
        private final String name;

        public SimpleObject(final String name) {
            this.name = name;
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

    private static final class ObjectWithMapOfMaps{
        private final Map<String, Map<String, String>> theMap = new HashMap<String, Map<String, String>>();

        private ObjectWithMapOfMaps() {
            final Map<String, String> innerMap = new HashMap<String, String>();
            innerMap.put("innerMapField", "theInnerMapFieldValue");
            theMap.put("theMapInsideTheMap", innerMap);
        }
    }
}
