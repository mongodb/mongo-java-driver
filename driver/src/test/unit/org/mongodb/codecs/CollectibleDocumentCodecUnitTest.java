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

import org.bson.BsonDocumentWriter;
import org.bson.types.BsonDocument;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.codecs.validators.FieldNameValidator;

import java.io.IOException;

public class CollectibleDocumentCodecUnitTest {
    private DocumentCodec codec;
    private BsonDocumentWriter writer;

    @Before
    public void setUp() throws Exception {
        writer = new BsonDocumentWriter(new BsonDocument());
        codec = new DocumentCodec(new FieldNameValidator());
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
