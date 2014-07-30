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

package com.mongodb.protocol;

import com.mongodb.codecs.DocumentCodec;
import com.mongodb.connection.ByteBufferOutputBuffer;
import com.mongodb.connection.SimpleBufferProvider;
import com.mongodb.operation.InsertRequest;
import com.mongodb.protocol.message.InsertMessage;
import com.mongodb.protocol.message.MessageSettings;
import com.mongodb.protocol.message.RequestMessage;
import org.bson.types.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;

import java.util.Arrays;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MaxMessageSizeTest {
    private ByteBufferOutputBuffer buffer;
    private InsertMessage<Document> message;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        Binary binary = new Binary(new byte[2048]);
        message = new InsertMessage<Document>("test.test", true, ACKNOWLEDGED,
                                              Arrays.asList(new InsertRequest<Document>(new Document("bytes", binary)),
                                                            new InsertRequest<Document>(new Document("bytes", binary)),
                                                            new InsertRequest<Document>(new Document("bytes", binary))),
                                              new DocumentCodec(), MessageSettings.builder().maxMessageSize(4500).build());
        buffer = new ByteBufferOutputBuffer(new SimpleBufferProvider());
    }

    @After
    public void tearDown() {
        buffer.close();
    }

    @Test
    public void testMaxDocumentSize() {
        RequestMessage next = message.encode(buffer);
        assertNotNull(next);
        assertNull(next.encode(buffer));
    }

}
