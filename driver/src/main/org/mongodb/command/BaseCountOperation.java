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

package org.mongodb.command;

import org.mongodb.Codec;
import org.mongodb.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.connection.BufferProvider;
import org.mongodb.operation.CommandResult;
import org.mongodb.operation.Find;

public abstract class BaseCountOperation {
    private final Count count;
    private final Codec<Document> codec;
    private final BufferProvider bufferProvider;

    public BaseCountOperation(final Find find, final MongoNamespace namespace, final Codec<Document> codec,
                              final BufferProvider bufferProvider) {
        this.count = new Count(find, namespace);
        this.codec = codec;
        this.bufferProvider = bufferProvider;
    }

    public Count getCount() {
        return count;
    }

    public Codec<Document> getCodec() {
        return codec;
    }

    public BufferProvider getBufferProvider() {
        return bufferProvider;
    }

    protected long getCount(final CommandResult commandResult) {
        return ((Number) commandResult.getResponse().get("n")).longValue();
    }
}
