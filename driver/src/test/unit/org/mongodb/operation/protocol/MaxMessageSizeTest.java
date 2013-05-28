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

package org.mongodb.operation.protocol;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.operation.MongoInsert;

import java.util.Arrays;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mongodb.Fixture.getBufferPool;

public class MaxMessageSizeTest {
    private PooledByteBufferOutputBuffer buffer;
    private MongoInsertMessage<Document> message;

    @Before
    public void setUp() {
        message = new MongoInsertMessage<Document>("test.test",
                new MongoInsert<Document>(
                        Arrays.asList(
                                new Document("bytes", new byte[2048]),
                                new Document("bytes", new byte[2048]),
                                new Document("bytes", new byte[2048])))
                        .writeConcern(WriteConcern.ACKNOWLEDGED),
                new DocumentCodec(), MessageSettings.builder().maxMessageSize(4500).build());
        buffer = new PooledByteBufferOutputBuffer(getBufferPool());
    }

    @After
    public void tearDown() {
        buffer.close();
    }

    @Test
    public void testMaxDocumentSize() {
        MongoRequestMessage next = message.encode(buffer);
        assertNotNull(next);
        assertNull(next.encode(buffer));
    }

}
