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

import static org.junit.Assert.fail;

@Ignore("Doesn't work on the command line because code coverage tool pokes things onto my objects")
public class PojoCodecTest {
    //CHECKSTYLE:OFF
    @Rule public JUnitRuleMockery context = new JUnitRuleMockery();
    //CHECKSTYLE:ON

    private BSONWriter bsonWriter;
    private PojoCodec pojoCodec;

    @Before
    public void setUp() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        bsonWriter = context.mock(BSONWriter.class);
        pojoCodec = new PojoCodec(PrimitiveCodecs.createDefault());
    }

    @Test
    public void shouldEncodeSimplePojo() {
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeString("MyName");
            oneOf(bsonWriter).writeEndDocument();

        }});
        ignoreJacocoInvocations(bsonWriter);

        pojoCodec.encode(bsonWriter, new SimpleObject("MyName"));
    }

    @Test
    public void shouldEncodePojoContainingOtherPojos() {
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeStartDocument("mySimpleObject");
            oneOf(bsonWriter).writeName("name");
            oneOf(bsonWriter).writeString("AnotherName");
            exactly(2).of(bsonWriter).writeEndDocument();

        }});
        ignoreJacocoInvocations(bsonWriter);

        pojoCodec.encode(bsonWriter, new NestedObject(new SimpleObject("AnotherName")));
    }

    @Test
    public void shouldEncodePojoContainingOtherPojosAndFields() {
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeName("intValue");
            oneOf(bsonWriter).writeInt32(98);
            oneOf(bsonWriter).writeStartDocument("mySimpleObject");
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
    @Ignore("not implemented")
    public void shouldSupportArrays() {
        context.checking(new Expectations() {{
            oneOf(bsonWriter).writeStartDocument();
            oneOf(bsonWriter).writeStartArray("theStringArray");
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
        context.checking(new Expectations() {{
            ignoring(writer).writeStartDocument("$jacocoData");
            ignoring(writer).writeStartArray("$jacocoData");
            ignoring(writer).writeEndDocument();
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

    private static class ObjectWithArray {
        private final String[] theStringArray = {"Uno", "Dos", "Tres"};
    }
}
