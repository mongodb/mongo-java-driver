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

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.operation.ReplaceRequest;
import org.bson.BsonBinaryWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;

import java.util.List;

/**
 * An update command message that handles full document replacements.
 *
 * @since 3.0
 * @mongodb.driver.manual manual/reference/command/insert/#dbcmd.update Update Command
 */
public class ReplaceCommandMessage extends BaseUpdateCommandMessage<ReplaceRequest> {
    /**
     * Construct an instance.
     *
     * @param namespace the namespace
     * @param ordered whether the inserts are ordered
     * @param writeConcern the write concern
     * @param replaceRequests the list of replace requests
     * @param settings the message settings
     */
    public ReplaceCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<ReplaceRequest> replaceRequests,
                                 final MessageSettings settings) {
        super(namespace, ordered, writeConcern, replaceRequests, settings);
    }

    @Override
    protected void writeUpdate(final BsonBinaryWriter writer, final ReplaceRequest update) {
        getBsonDocumentCodec().encode(writer, update.getReplacement(),
                                      EncoderContext.builder().isEncodingCollectibleDocument(true).build());
    }

    @Override
    protected ReplaceCommandMessage createNextMessage(final List<ReplaceRequest> remainingUpdates) {
        return new ReplaceCommandMessage(getWriteNamespace(), isOrdered(), getWriteConcern(), remainingUpdates, getSettings());
    }

    @Override
    protected FieldNameValidator getUpdateFieldNameValidator() {
        return new CollectibleDocumentFieldNameValidator();
    }
}
