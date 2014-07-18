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

package org.mongodb.protocol;

import com.mongodb.codecs.DocumentCodec;
import com.mongodb.connection.ByteBufferOutputBuffer;
import org.bson.BsonSerializationException;
import org.bson.types.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.SimpleBufferProvider;
import org.mongodb.operation.InsertRequest;
import org.mongodb.protocol.message.InsertMessage;
import org.mongodb.protocol.message.MessageSettings;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static java.util.Arrays.asList;

@SuppressWarnings("unchecked")
public class MaxDocumentSizeTest {
    private ByteBufferOutputBuffer buffer;
    private InsertMessage<Document> message;

    @Before
    public void setUp() {
        message = new InsertMessage<Document>("test.test", true, ACKNOWLEDGED,
                                              asList(new InsertRequest<Document>(new Document("bytes", new Binary(new byte[2048])))),
                                              new DocumentCodec(), MessageSettings.builder().maxDocumentSize(1024).build());
        buffer = new ByteBufferOutputBuffer(new SimpleBufferProvider());
    }

    @After
    public void tearDown() {
        buffer.close();
    }

    @Test(expected = BsonSerializationException.class)
    public void testMaxDocumentSize() {
        message.encode(buffer);
    }
}
