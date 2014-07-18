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
import com.mongodb.operation.ReplaceRequest;
import org.bson.BsonBinaryWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.mongodb.MongoNamespace;

import java.util.List;

public class ReplaceCommandMessage<T> extends BaseUpdateCommandMessage<ReplaceRequest<T>> {
    private final Encoder<T> encoder;

    public ReplaceCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                 final List<ReplaceRequest<T>> replaceRequests,
                                 final Encoder<T> encoder, final MessageSettings settings) {
        super(namespace, ordered, writeConcern, replaceRequests, settings);
        this.encoder = encoder;
    }

    @Override
    protected void writeUpdate(final BsonBinaryWriter writer, final ReplaceRequest<T> update) {
        encoder.encode(writer, update.getReplacement(), EncoderContext.builder().isEncodingCollectibleDocument(true).build());
    }

    @Override
    protected ReplaceCommandMessage<T> createNextMessage(final List<ReplaceRequest<T>> remainingUpdates) {
        return new ReplaceCommandMessage<T>(getWriteNamespace(), isOrdered(), getWriteConcern(), remainingUpdates,
                                            encoder, getSettings());
    }

    @Override
    protected FieldNameValidator getUpdateFieldNameValidator() {
        return new CollectibleDocumentFieldNameValidator();
    }
}
