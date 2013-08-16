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

package org.mongodb.operation;

import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.protocol.MessageSettings;
import org.mongodb.operation.protocol.ReplaceMessage;
import org.mongodb.operation.protocol.RequestMessage;

import java.util.Arrays;

public class AsyncReplaceOperation<T> extends AsyncWriteOperation {
    private final Replace<T> replace;
    private final Encoder<Document> queryEncoder;
    private final Encoder<T> encoder;

    public AsyncReplaceOperation(final MongoNamespace namespace, final Replace<T> replace, final Encoder<Document> queryEncoder,
                                 final Encoder<T> encoder, final BufferProvider bufferProvider) {
        super(namespace, bufferProvider);
        this.replace = replace;
        this.queryEncoder = queryEncoder;
        this.encoder = encoder;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected RequestMessage createRequestMessage(final MessageSettings settings) {
        return new ReplaceMessage<T>(getNamespace().getFullName(), Arrays.asList(replace), queryEncoder, encoder, settings);
    }

    @Override
    public Replace<T> getWrite() {
        return replace;
    }

    @Override
    public WriteConcern getWriteConcern() {
        return replace.getWriteConcern();
    }
}
