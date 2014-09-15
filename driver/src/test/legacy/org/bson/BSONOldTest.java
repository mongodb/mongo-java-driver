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

package org.bson;

import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.CodeWScope;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.util.Util.hexMD5;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

@SuppressWarnings({"unchecked", "rawtypes"})
public class BSONOldTest {

    @Before
    public void setUp() {
        BSON.clearAllHooks();        
    }

    @After
    public void tearDown() {
        BSON.clearAllHooks();
    }

    private void test(final BSONObject o, final int size, final String hash) throws IOException {
        BSONEncoder e = new BasicBSONEncoder();
        OutputBuffer buf = new BasicOutputBuffer();
        e.set(buf);
        e.putObject(o);
        assertEquals(size, buf.size());
        assertEquals(hash, hexMD5(buf.toByteArray()));
        e.done();

        BSONDecoder d = new BasicBSONDecoder();
        BSONCallback cb = new BasicBSONCallback();
        int s = d.decode(new ByteArrayInputStream(buf.toByteArray()), cb);
        assertEquals(size, s);

        OutputBuffer buf2 = new BasicOutputBuffer();
        e.set(buf2);
        e.putObject((BSONObject) cb.get());
        assertEquals(size, buf2.size());
        assertEquals(hash, hexMD5(buf2.toByteArray()));
    }

    @Test
    public void testBasic1() throws IOException {
        test(new BasicBSONObject("x", true), 9, "6fe24623e4efc5cf07f027f9c66b5456");
        test(new BasicBSONObject("x", null), 8, "12d43430ff6729af501faf0638e68888");
        test(new BasicBSONObject("x", 5.2), 16, "aaeeac4a58e9c30eec6b0b0319d0dff2");
        test(new BasicBSONObject("x", "eliot"), 18, "331a3b8b7cbbe0706c80acdb45d4ebbe");
        test(new BasicBSONObject("x", 5.2).append("y", "truth")
                                          .append("z", 1.1),
             40, "7c77b3a6e63e2f988ede92624409da58");

        test(new BasicBSONObject("a", new BasicBSONObject("b", 1.1)), 24, "31887a4b9d55cd9f17752d6a8a45d51f");
        test(new BasicBSONObject("x", 5.2).append("y", new BasicBSONObject("a", "eliot").append("b", true))
                                          .append("z", null),
             44, "b3de8a0739ab329e7aea138d87235205");
        test(new BasicBSONObject("x", 5.2).append("y", new Object[]{"a", "eliot", "b", true})
                                          .append("z", null),
             62, "cb7bad5697714ba0cbf51d113b6a0ee8");
        test(new BasicBSONObject("x", 4), 12, "d1ed8dbf79b78fa215e2ded74548d89d");
    }

    @Test
    public void testArray() throws IOException {
        test(new BasicBSONObject("x", new int[]{1, 2, 3, 4}), 41, "e63397fe37de1349c50e1e4377a45e2d");
    }

    @Test
    public void testCode() throws IOException {
        BSONObject scope = new BasicBSONObject("x", 1);
        CodeWScope c = new CodeWScope("function() { x += 1; }", scope);
        BSONObject document = new BasicBSONObject("map", c);
        test(document, 53, "52918d2367533165bfc617df50335cbb");
    }

    @Test
    public void testBinary() throws IOException {
        byte[] data = new byte[10000];
        for (int i = 0; i < 10000; i++) {
            data[i] = 1;
        }
        BSONObject document = new BasicBSONObject("bin", data);
        test(document, 10015, "1d439ba5b959ecfe297a7862bf95bc10");
    }

    @Test
    public void testOBBig1() {
        BasicOutputBuffer a = new BasicOutputBuffer();
        StringBuilder b = new StringBuilder();
        for (final String x : prepareData()) {
            a.write(x.getBytes());
            b.append(x);
        }
        assertEquals(new String(a.toByteArray(), Charset.forName("UTF-8")), b.toString());
    }

    private List<String> prepareData() {
        List<String> data = new ArrayList<String>();

        for (int x = 8; x < 2048; x *= 2) {
            StringBuilder buf = new StringBuilder();
            while (buf.length() < x) {
                buf.append(x);
            }
            data.add(buf.toString());
        }
        return data;
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldClearCustomEncoders() throws IOException {
        // given
        BSON.addEncodingHook(TestDate.class, new TestDateTransformer());
        BSONEncoder encoder = new BasicBSONEncoder();
        encoder.set(new BasicOutputBuffer());
        
        // when
        BSON.clearEncodingHooks();
        encoder.putObject(new BasicBSONObject("date", new TestDate(2009, 1, 23, 10, 53, 42)));
    }

    @Test
    public void shouldTransformTestDateToUtilDateWithTestDateTransformer() throws IOException {
        // given
        Transformer transformer = new TestDateTransformer();

        // when
        Object transformedDate = transformer.transform(new TestDate(2009, 1, 23, 10, 53, 42));
        
        // then
        assertThat(transformedDate, is(instanceOf(java.util.Date.class)));
    }

    @Test
    public void shouldUseCustomEncodersWhenDecodingObjectOfRegisteredClass() throws IOException {
        // given
        StubTransformer stubTransformer = new StubTransformer();
        BSON.addEncodingHook(TestDate.class, stubTransformer);
        BSONEncoder encoder = new BasicBSONEncoder();
        encoder.set(new BasicOutputBuffer());
        
        BSONObject document = new BasicBSONObject("date", new TestDate(2009, 1, 23, 10, 53, 42));
        
        // when
        encoder.putObject(document);
        encoder.done();
        
        // then
        assertThat(stubTransformer.transformCalled, is(true));
    }

    @Test
    public void shouldReturnRegisteredCustomEncoders() throws IOException {
        // when
        Transformer transformer = new TestDateTransformer();
        BSON.addEncodingHook(TestDate.class, transformer);

        // then
        assertThat(BSON.hasEncodeHooks(), is(true));

        List<Transformer> encodingHooks = BSON.getEncodingHooks(TestDate.class);
        assertThat(encodingHooks, is(notNullValue()));
        assertThat(encodingHooks, is(asList(transformer)));
    }

    @Test
    public void shouldRemoveSpecificRegisteredCustomEncoders() throws IOException {
        Transformer transformer = new TestDateTransformer();
        BSON.addEncodingHook(TestDate.class, transformer);

        // when
        BSON.removeEncodingHook(TestDate.class, transformer);
        
        // then
        assertThat(BSON.getEncodingHooks(TestDate.class), not(contains(transformer)));
    }

    @Test
    public void shouldClearCustomDecoders() throws IOException {
        // given
        BSON.addDecodingHook(Date.class, new TestDateTransformer());
        byte[] encodedDocument = encodeDocumentToByteArray(new BasicBSONObject("date", new Date()));
        BSONCallback bsonCallback = new BasicBSONCallback();

        // when
        BSON.clearDecodingHooks();
        new BasicBSONDecoder().decode(new ByteArrayInputStream(encodedDocument), bsonCallback);

        // then
        BSONObject decodedDocument = (BSONObject) bsonCallback.get();
        assertThat(decodedDocument.get("date"), is(instanceOf(java.util.Date.class)));
    }

    @Test
    public void shouldUseCustomDecodersWhenDecodingObjectOfRegisteredClass() throws IOException {
        // given
        @SuppressWarnings("deprecation")
        byte[] encodedDocument = encodeDocumentToByteArray(new BasicBSONObject("date", new Date(2009, 01, 23, 10, 53, 42)));
        BSONCallback bsonCallback = new BasicBSONCallback();

        // when
        BSON.addDecodingHook(Date.class, new TestDateTransformer());
        new BasicBSONDecoder().decode(new ByteArrayInputStream(encodedDocument), bsonCallback);

        // then
        BSONObject decodedDocument = (BSONObject) bsonCallback.get();
        assertThat(decodedDocument.get("date"), is(instanceOf(TestDate.class)));
        assertThat((TestDate) decodedDocument.get("date"), is(new TestDate(2009, 01, 23, 10, 53, 42)));
    }

    @Test
    public void shouldReturnRegisteredCustomDecoders() throws IOException {
        // when
        Transformer transformer = new TestDateTransformer();
        BSON.addDecodingHook(Date.class, transformer);

        // then
        assertThat(BSON.hasDecodeHooks(), is(true));
        
        List<Transformer> decodingHooks = BSON.getDecodingHooks(Date.class);
        assertThat(decodingHooks, is(notNullValue()));
        assertThat(decodingHooks, is(asList(transformer)));
    }

    @Test
    public void shouldRemoveSpecificRegisteredCustomDecoders() throws IOException {
        // given
        Transformer transformer = new TestDateTransformer();
        BSON.addDecodingHook(Date.class, transformer);

        // when
        BSON.removeDecodingHook(Date.class, transformer);

        // expect
        assertThat(BSON.getDecodingHooks(Date.class), not(contains(transformer)));
    }

    @Test
    public void testEquals() {
        assertNotEquals(new BasicBSONObject("a", 1111111111111111111L), new BasicBSONObject("a", 1111111111111111112L));
        assertNotEquals(new BasicBSONObject("a", 100.1D), new BasicBSONObject("a", 100.2D));
        assertNotEquals(new BasicBSONObject("a", 100.1F), new BasicBSONObject("a", 100.2F));
        assertEquals(new BasicBSONObject("a", 100.1D), new BasicBSONObject("a", 100.1D));
        assertEquals(new BasicBSONObject("a", 100.1F), new BasicBSONObject("a", 100.1F));
        assertEquals(new BasicBSONObject("a", 100L), new BasicBSONObject("a", 100L));
    }

    @Test
    public void testRandomRoundTrips() {
        roundTrip(new BasicBSONObject("a", ""));
        roundTrip(new BasicBSONObject("a", "a"));
        roundTrip(new BasicBSONObject("a", "b"));
    }

    private byte[] encodeDocumentToByteArray(final BSONObject document) {
        OutputBuffer outputBuffer = new BasicOutputBuffer();
        BSONEncoder encoder = new BasicBSONEncoder();
        encoder.set(outputBuffer);
        encoder.putObject(document);
        encoder.done();
        return outputBuffer.toByteArray();
    }

    private void roundTrip(final BSONObject o) {
        assertEquals(o, BSON.decode(BSON.encode(o)));
    }

    private class TestDate {
        private final int year;
        private final int month;
        private final int date;
        private final int hour;
        private final int minute;
        private final int second;

        public TestDate(final int year, final int month, final int date, final int hour, final int minute, final int second) {
            this.year = year;
            this.month = month;
            this.date = date;
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof TestDate)) {
                return false;
            }

            TestDate otherTestDate = (TestDate) other;
            return (otherTestDate.year == this.year
                    && otherTestDate.month == this.month
                    && otherTestDate.date == this.date
                    && otherTestDate.hour == this.hour
                    && otherTestDate.minute == this.minute
                    && otherTestDate.second == this.second
            );
        }

        @Override
        public int hashCode() {
            int result = year;
            result = 31 * result + month;
            result = 31 * result + date;
            result = 31 * result + hour;
            result = 31 * result + minute;
            result = 31 * result + second;
            return result;
        }

        @Override
        public String toString() {
            return year + "-" + month + "-" + date + " " + hour + ":" + minute + ":" + second;
        }
    }
    
    private static class StubTransformer implements Transformer {
        private boolean transformCalled = false;

        @Override
        public Object transform(final Object o) {
            transformCalled = true;
            return true;
        }
    } 

    private class TestDateTransformer implements Transformer {
        @SuppressWarnings("deprecation")
        public Object transform(final Object o) {
            if (o instanceof TestDate) {
                TestDate td = (TestDate) o;
                return new java.util.Date(td.year, td.month, td.date, td.hour, td.minute, td.second);
            } else if (o instanceof java.util.Date) {
                Date d = (Date) o;
                return new TestDate(d.getYear(), d.getMonth(), d.getDate(), d.getHours(), d.getMinutes(), d.getSeconds());
            } else {
                return o;
            }
        }
    }
}
