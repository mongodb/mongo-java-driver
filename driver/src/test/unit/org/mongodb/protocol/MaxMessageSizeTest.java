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

package org.mongodb.protocol;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.operation.Insert;
import org.mongodb.protocol.message.InsertMessage;
import org.mongodb.protocol.message.MessageSettings;
import org.mongodb.protocol.message.RequestMessage;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.WriteConcern.ACKNOWLEDGED;

public class MaxMessageSizeTest {
    private PooledByteBufferOutputBuffer buffer;
    private InsertMessage<Document> message;

    @Before
    public void setUp() {
        message = new InsertMessage<Document>("test.test",
                                              new Insert<Document>(ACKNOWLEDGED,
                                                                   asList(new Document("bytes", new byte[2048]),
                                                                          new Document("bytes", new byte[2048]),
                                                                          new Document("bytes", new byte[2048]))
                                              ),
                                              new DocumentCodec(),
                                              MessageSettings.builder().maxMessageSize(4500).build());
        buffer = new PooledByteBufferOutputBuffer(getBufferProvider());
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
