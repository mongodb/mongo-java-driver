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

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class LazyBSONDecoderTest {
    private BSONDecoder bsonDecoder;

    @Before
    public void setUp() {
        bsonDecoder = new LazyBSONDecoder();
    }

    @Test
    public void testDecodingFromInputStream() throws IOException {
        InputStream is = new ByteArrayInputStream(new byte[]{12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0});
        BSONObject document = bsonDecoder.readObject(is);
        assertNotNull(document);
        assertThat(document, instanceOf(LazyBSONObject.class));
        assertEquals(1, document.keySet().size());
        assertThat(document.keySet(), hasItems("a"));
        assertEquals(1, document.get("a"));
    }

    @Test
    public void testDecodingFromByteArray() throws IOException {
        byte[] bytes = new byte[]{12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0};
        BSONObject document = bsonDecoder.readObject(bytes);
        assertNotNull(document);
        assertThat(document, instanceOf(LazyBSONObject.class));
        assertEquals(1, document.keySet().size());
        assertThat(document.keySet(), hasItems("a"));
        assertEquals(1, document.get("a"));
    }

    @Test(expected = BSONException.class)
    public void testDecodingFromInvalidInput() {
        byte[] bytes = new byte[]{16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0};
        bsonDecoder.readObject(bytes);
    }

}
