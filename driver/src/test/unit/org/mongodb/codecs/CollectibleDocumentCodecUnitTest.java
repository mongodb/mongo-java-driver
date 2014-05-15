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

package org.mongodb.codecs;

import org.bson.BSONBinaryWriter;
import org.bson.BSONBinaryWriterSettings;
import org.bson.BSONWriter;
import org.bson.BSONWriterSettings;
import org.bson.io.BasicOutputBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;

import java.io.IOException;

public class CollectibleDocumentCodecUnitTest {
    private CollectibleDocumentCodec codec;
    private BSONWriter writer;

    @Before
    public void setUp() throws Exception {
        writer = new BSONBinaryWriter(new BSONWriterSettings(100),
                                      new BSONBinaryWriterSettings(1024 * 1024),
                                      new BasicOutputBuffer(), true);
        codec = new CollectibleDocumentCodec(new ObjectIdGenerator());
    }

    @After
    public void tearDown() {
        writer.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectFieldNamesWithDotsInForDocumentsThatAreSaved() throws IOException {
        Document document = new Document("x.y", 1);

        codec.encode(writer, document);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectFieldNamesWithDotsInForNestedDocumentsThatAreSaved() throws IOException {
        Document document = new Document("x", new Document("a.b", 1));

        codec.encode(writer, document);
    }

}
