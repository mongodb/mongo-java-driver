/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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

/**
 *
 */
package com.google.code.morphia.mapping;

import com.google.code.morphia.TestBase;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Serialized;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Uwe Schaefer, (us@thomas-daily.de)
 */
public class TestSerializerTest extends TestBase {

    private static final String TEST_TEXT = "In 1970, the British Empire lay in ruins, "
                                            + "and foreign nationalists frequented the streets - many of them " +
                                            "Hungarians (not the streets - the foreign"
                                            + " nationals). Anyway, many of these Hungarians went into tobacconist's " +
                                            "shops to buy cigarettes.... ";

    @Test
    public final void testSerialize() throws IOException, ClassNotFoundException {
        final byte[] test = new byte[2048];
        final byte[] stringBytes = TEST_TEXT.getBytes();
        System.arraycopy(stringBytes, 0, test, 0, stringBytes.length);

        byte[] ser = Serializer.serialize(test, false);
        byte[] after = (byte[]) Serializer.deserialize(ser, false);
        Assert.assertTrue(ser.length > 2048);
        Assert.assertTrue(after.length == 2048);
        Assert.assertTrue(new String(after).startsWith(TEST_TEXT));

        ser = Serializer.serialize(test, true);
        after = (byte[]) Serializer.deserialize(ser, true);
        Assert.assertTrue(ser.length < 2048);
        Assert.assertTrue(after.length == 2048);
        Assert.assertTrue(new String(after).startsWith(TEST_TEXT));
    }

    @Test
    public final void testSerializedAttribute() throws IOException, ClassNotFoundException {
        final byte[] test = new byte[2048];
        final byte[] stringBytes = TEST_TEXT.getBytes();
        System.arraycopy(stringBytes, 0, test, 0, stringBytes.length);

        E e = new E();
        e.payload1 = test;
        e.payload2 = test;

        ds.save(e);
        e = ds.get(e);

        Assert.assertTrue(e.payload1.length == 2048);
        Assert.assertTrue(new String(e.payload1).startsWith(TEST_TEXT));

        Assert.assertTrue(e.payload2.length == 2048);
        Assert.assertTrue(new String(e.payload2).startsWith(TEST_TEXT));
    }

    private static class E {
        @Id
        private ObjectId id;
        @Serialized
        private byte[] payload1;
        @Serialized()
        private byte[] payload2;
    }
}
