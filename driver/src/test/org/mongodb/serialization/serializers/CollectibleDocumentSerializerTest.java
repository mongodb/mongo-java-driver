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
 *
 */

package org.mongodb.serialization.serializers;

import org.bson.BSONBinaryWriter;
import org.bson.BSONWriter;
import org.bson.BinaryWriterSettings;
import org.bson.BsonWriterSettings;
import org.bson.io.BasicOutputBuffer;
import org.bson.types.Document;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.serialization.IdGenerator;
import org.mongodb.serialization.PrimitiveSerializers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

// TODO: Write tests
public class CollectibleDocumentSerializerTest {

    private CollectibleDocumentSerializer serializer;
    private BasicOutputBuffer outputBuffer;
    private BSONWriter writer;

    @Before
    public void setUp() {
        serializer = new CollectibleDocumentSerializer(PrimitiveSerializers.createDefault(), new IdGenerator() {
            @Override
            public Object generate() {
                return 1;
            }
        });
        outputBuffer = new BasicOutputBuffer();
        writer = new BSONBinaryWriter(new BsonWriterSettings(10), new BinaryWriterSettings(100), outputBuffer);
        writer.writeStartDocument();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructor() {
        new CollectibleDocumentSerializer(PrimitiveSerializers.createDefault(), null);
    }

    @Test
    public void testGetId() {
        assertEquals(1, serializer.getId(new Document("_id", 1)));
        assertNull(serializer.getId(new Document()));
    }

    @Test
    public void testFieldValidationSuccess() {
        serializer.validateFieldName("ok");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullFieldNameValidation() {
        serializer.validateFieldName(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldNameWithDotsValidation() {
        serializer.validateFieldName("1.2");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFieldNameStartsWithDollarValidation() {
        serializer.validateFieldName("$1");
    }

    @Test
    public void testSkipField() {
        assertFalse(serializer.skipField("ok"));
        assertTrue(serializer.skipField("_id"));
    }

    @Test
    public void testBeforeFieldsWithGeneratedId() {
        Document document = new Document();
        serializer.beforeFields(writer, document, null);
        assertEquals(1, document.get("_id"));
        assertEquals(13, outputBuffer.size());    // TODO: Not such an accurate test
    }

    @Test
    public void testBeforeFieldsWithExistingId() {
        Document document = new Document("_id", "Hi mom");
        serializer.beforeFields(writer, document, null);
        assertEquals("Hi mom", document.get("_id"));
        assertEquals(20, outputBuffer.size());    // TODO: Not such an accurate test
    }
}