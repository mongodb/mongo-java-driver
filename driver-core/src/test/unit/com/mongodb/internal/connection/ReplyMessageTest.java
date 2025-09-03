/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import com.mongodb.MongoInternalException;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.connection.MessageHelper.buildReply;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReplyMessageTest {

    @Test
    public void shouldThrowExceptionIfRequestIdDoesNotMatchResponseTo() {
        int badResponseTo = 34565;
        int expectedResponseTo = 5;

        ResponseBuffers responseBuffers = buildReply(badResponseTo, "{ok: 1}", 0);

        assertThrows(MongoInternalException.class, () ->
                new ReplyMessage<>(responseBuffers, new BsonDocumentCodec(), expectedResponseTo));
    }
}
