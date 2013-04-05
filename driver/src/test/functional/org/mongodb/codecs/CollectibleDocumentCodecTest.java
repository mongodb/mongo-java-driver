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

import org.bson.BSONBinaryWriter;
import org.bson.BSONBinaryWriterSettings;
import org.bson.BSONWriter;
import org.bson.BSONWriterSettings;
import org.bson.io.BasicOutputBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.IdGenerator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

// TODO: Write tests
public class CollectibleDocumentCodecTest {

    private CollectibleDocumentCodec codec;
    private BasicOutputBuffer outputBuffer;
    private BSONWriter writer;

    @Before
    public void setUp() {
        codec = new CollectibleDocumentCodec(PrimitiveCodecs.createDefault(), new IdGenerator() {
            @Override
            public Object generate() {
                return 1;
            }
        });
        outputBuffer = new BasicOutputBuffer();
        writer = new BSONBinaryWriter(new BSONWriterSettings(10), new BSONBinaryWriterSettings(100), outputBuffer);
        writer.writeStartDocument();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor() {
        new CollectibleDocumentCodec(PrimitiveCodecs.createDefault(), null);
    }

    @Test
    public void testGetId() {
        assertEquals(1, codec.getId(new Document("_id", 1)));
        assertNull(codec.getId(new Document()));
    }

    @Test
    public void testFieldValidationSuccess() {
        codec.validateFieldName("ok");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullFieldNameValidation() {
        codec.validateFieldName(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldNameWithDotsValidation() {
        codec.validateFieldName("1.2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldNameStartsWithDollarValidation() {
        codec.validateFieldName("$1");
    }

    @Test
    public void testSkipField() {
        assertFalse(codec.skipField("ok"));
        assertTrue(codec.skipField("_id"));
    }

    @Test
    public void testBeforeFieldsWithGeneratedId() {
        final Document document = new Document();
        codec.beforeFields(writer, document);
        assertEquals(1, document.get("_id"));
        assertEquals(13, outputBuffer.size());    // TODO: Not such an accurate test
    }

    @Test
    public void testBeforeFieldsWithExistingId() {
        final Document document = new Document("_id", "Hi mom");
        codec.beforeFields(writer, document);
        assertEquals("Hi mom", document.get("_id"));
        assertEquals(20, outputBuffer.size());    // TODO: Not such an accurate test
    }
}