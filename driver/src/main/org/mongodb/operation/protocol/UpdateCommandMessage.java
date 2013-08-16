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
import org.mongodb.operation.Update;

import java.util.List;

public class UpdateCommandMessage extends BaseUpdateCommandMessage<Update> {
    public UpdateCommandMessage(final MongoNamespace namespace, final WriteConcern writeConcern, final List<Update> updates,
                                final Encoder<Document> commandEncoder, final MessageSettings messageSettings) {
        super(namespace, writeConcern, updates, commandEncoder, messageSettings);
    }

    protected void writeUpdate(final BSONBinaryWriter writer, final Update update) {
        getCommandEncoder().encode(writer, update.getUpdateOperations());
    }

    protected UpdateCommandMessage createNextMessage(final List<Update> remainingUpdates) {
        return new UpdateCommandMessage(getWriteNamespace(), getWriteConcern(), remainingUpdates, getCommandEncoder(), getSettings());
    }
}
