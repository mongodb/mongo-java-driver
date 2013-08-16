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

import org.bson.BSONBinaryWriter;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.operation.Replace;

import java.util.List;

public class ReplaceCommandMessage<T> extends BaseUpdateCommandMessage<Replace<T>> {
    private final Encoder<T> encoder;

    public ReplaceCommandMessage(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Replace<T>> replaces,
                                 final Encoder<Document> queryEncoder, final Encoder<T> encoder, final MessageSettings settings) {
        super(namespace, writeConcern, replaces, queryEncoder, settings);
        this.encoder = encoder;
    }

    @Override
    protected void writeUpdate(final BSONBinaryWriter writer, final Replace<T> update) {
        encoder.encode(writer, update.getReplacement());
    }

    @Override
    protected ReplaceCommandMessage<T> createNextMessage(final List<Replace<T>> remainingUpdates) {
        return new ReplaceCommandMessage<T>(getWriteNamespace(), getWriteConcern(), remainingUpdates, getCommandEncoder(),
                encoder, getSettings());
    }
}
