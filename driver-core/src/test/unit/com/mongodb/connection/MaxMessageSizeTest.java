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

package com.mongodb.connection;

import com.mongodb.bulk.InsertRequest;
import com.mongodb.internal.connection.NoOpSessionContext;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static com.mongodb.WriteConcern.ACKNOWLEDGED;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MaxMessageSizeTest {
    private ByteBufferBsonOutput buffer;
    private InsertMessage message;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        BsonBinary binary = new BsonBinary(new byte[2048]);
        message = new InsertMessage("test.test", true, ACKNOWLEDGED,
                                    Arrays.asList(new InsertRequest(new BsonDocument("bytes", binary)),
                                                  new InsertRequest(new BsonDocument("bytes", binary)),
                                                  new InsertRequest(new BsonDocument("bytes", binary))),
                                    MessageSettings.builder().maxMessageSize(4500).build());
        buffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
    }

    @After
    public void tearDown() {
        buffer.close();
    }

    @Test
    public void testMaxDocumentSize() {
        message.encode(buffer, NoOpSessionContext.INSTANCE);
        RequestMessage next = message.getEncodingMetadata().getNextMessage();
        assertNotNull(next);
        next.encode(buffer, NoOpSessionContext.INSTANCE);
        assertNull(next.getEncodingMetadata().getNextMessage());
    }

}
