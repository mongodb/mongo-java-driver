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
import org.bson.BsonSerializationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class MaxDocumentSizeTest {
    private ByteBufferBsonOutput buffer;
    private InsertMessage message;

    @Before
    public void setUp() {
        message = new InsertMessage("test.test",
                                           new InsertRequest(new BsonDocument("bytes", new BsonBinary(new byte[2048]))),
                                           MessageSettings.builder().maxDocumentSize(1024).build());
        buffer = new ByteBufferBsonOutput(new SimpleBufferProvider());
    }

    @After
    public void tearDown() {
        buffer.close();
    }

    @Test(expected = BsonSerializationException.class)
    public void testMaxDocumentSize() {
        message.encode(buffer, NoOpSessionContext.INSTANCE);
    }
}
