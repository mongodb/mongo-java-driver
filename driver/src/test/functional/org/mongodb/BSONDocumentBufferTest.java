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

package org.mongodb;

import org.junit.Test;
import org.mongodb.codecs.DocumentCodec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

// A unit test
public class BSONDocumentBufferTest {
    @Test
    public void testRoundTrip() {
        Document document = new Document("a", 1).append("b", 2);

        DocumentCodec documentCodec = new DocumentCodec(PrimitiveCodecs.createDefault());

        BSONDocumentBuffer buffer = new BSONDocumentBuffer(document, documentCodec);

        assertNotNull(buffer.getByteBuffer());
        assertEquals(document, buffer.decode(documentCodec));
    }
}
