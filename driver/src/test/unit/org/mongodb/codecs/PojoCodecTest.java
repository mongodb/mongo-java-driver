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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.mongodb.json.JSONWriter;

import java.io.StringWriter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PojoCodecTest {
    private PojoCodec pojoCodec;
    private BSONWriter bsonWriter = mock(BSONWriter.class, Mockito.withSettings().verboseLogging());
    private PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();

    @Before
    public void setUp() {
        pojoCodec = new PojoCodec(primitiveCodecs);
    }

    @Test
    public void shouldEncodeString() {
        pojoCodec.encode(bsonWriter, "Bob");

        verify(bsonWriter).writeString("Bob");
    }

    @Test
    public void shouldEncodeInt() {
        pojoCodec.encode(bsonWriter, 32);

        verify(bsonWriter).writeInt32(32);
    }

    @Test
    @Ignore("not implemented")
    public void shouldEncodeSimplePojo() {
        pojoCodec.encode(bsonWriter, new SimpleObject("MyName"));

        verify(bsonWriter).writeStartDocument();
        verify(bsonWriter).writeName("name");
        verify(bsonWriter).writeString("MyName");
        verify(bsonWriter).writeEndDocument();
    }

    @Test
    @Ignore("not implemented")
    public void shouldEncodeSimplePojo2() {
        final StringWriter writer = new StringWriter();
        pojoCodec.encode(new JSONWriter(writer), new SimpleObject("MyName"));

        System.out.println(writer.toString());
        verify(bsonWriter).writeName("name");
        verify(bsonWriter).writeString("MyName");
    }

    @Test
    @Ignore("not implemented")
    public void shouldSupportArrays() {
        Assert.fail("Not implemented");
    }

    @Test
    @Ignore("not implemented")
    public void shouldEncodeIds() {
        Assert.fail("Not implemented");
    }

    @Test
    @Ignore("not implemented")
    public void shouldThrowAnExceptionWhenItCannotEncodeAField() {
        Assert.fail("Not implemented");
    }


    private static class SimpleObject {
        //CHECKSTYLE:OFF
        private final String name;
        //CHECKSTYLE:ON

        public SimpleObject(final String name) {
            this.name = name;
        }
    }
}
