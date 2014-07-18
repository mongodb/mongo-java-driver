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

package com.mongodb.protocol.message;

import com.mongodb.WriteConcern;
import org.bson.BsonBinaryWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.UpdateRequest;

import java.util.List;

public class UpdateCommandMessage extends BaseUpdateCommandMessage<UpdateRequest> {
    public UpdateCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                final List<UpdateRequest> updates,
                                final MessageSettings messageSettings) {
        super(namespace, ordered, writeConcern, updates, messageSettings);
    }

    protected void writeUpdate(final BsonBinaryWriter writer, final UpdateRequest update) {
        getBsonDocumentCodec().encode(writer, update.getUpdateOperations(), EncoderContext.builder().build());
    }

    protected UpdateCommandMessage createNextMessage(final List<UpdateRequest> remainingUpdates) {
        return new UpdateCommandMessage(getWriteNamespace(), isOrdered(), getWriteConcern(), remainingUpdates, getSettings());
    }

    @Override
    protected FieldNameValidator getUpdateFieldNameValidator() {
        return new UpdateFieldNameValidator();
    }
}
