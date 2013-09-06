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
import org.mongodb.MongoInvalidDocumentException;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.PooledByteBufferOutputBuffer;
import org.mongodb.operation.Insert;
import org.mongodb.protocol.message.InsertMessage;
import org.mongodb.protocol.message.MessageSettings;

import static org.mongodb.Fixture.getBufferProvider;
import static org.mongodb.WriteConcern.ACKNOWLEDGED;

public class MaxDocumentSizeTest {
    private PooledByteBufferOutputBuffer buffer;
    private InsertMessage<Document> message;

    @Before
    public void setUp() {
        message = new InsertMessage<Document>("test.test",
                                              new Insert<Document>(ACKNOWLEDGED, new Document("bytes", new byte[2048])),
                                              new DocumentCodec(),
                                              MessageSettings.builder().maxDocumentSize(1024).build());
        buffer = new PooledByteBufferOutputBuffer(getBufferProvider());
    }

    @After
    public void tearDown() {
        buffer.close();
    }

    @Test(expected = MongoInvalidDocumentException.class)
    public void testMaxDocumentSize() {
        message.encode(buffer);
    }
}
