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
import com.mongodb.operation.RemoveRequest;
import org.bson.BsonBinaryWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.bson.io.BsonOutput;

import java.util.Collections;
import java.util.List;

/**
 * A message for the delete command.
 *
 * @mongodb.driver.manual manual/reference/command/insert/#dbcmd.delete Delete Command
 * @since 3.0
 */
public class DeleteCommandMessage extends BaseWriteCommandMessage {
    private final List<RemoveRequest> deletes;

    /**
     * Construct an instance.
     *
     * @param namespace the namespace
     * @param ordered whether the writes are ordered
     * @param writeConcern the write concern
     * @param deletes the list of delete requests
     * @param settings the message settings
     */
    public DeleteCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                                final List<RemoveRequest> deletes, final MessageSettings settings) {
        super(namespace, ordered, writeConcern, settings);
        this.deletes = deletes;
    }

    @Override
    public int getItemCount() {
        return deletes.size();
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        return new NoOpFieldNameValidator();
    }

    /**
     * Gets the list of requests.
     *
     * @return the list of requests
     */
    public List<RemoveRequest> getRequests() {
        return Collections.unmodifiableList(deletes);
    }

    @Override
    protected String getCommandName() {
        return "delete";
    }

    @Override
    protected BaseWriteCommandMessage writeTheWrites(final BsonOutput bsonOutput, final int commandStartPosition,
                                                     final BsonBinaryWriter writer) {
        DeleteCommandMessage nextMessage = null;
        writer.writeStartArray("deletes");
        for (int i = 0; i < deletes.size(); i++) {
            writer.mark();
            RemoveRequest removeRequest = deletes.get(i);
            writer.writeStartDocument();
            writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
            writer.writeName("q");
            getBsonDocumentCodec().encode(writer, removeRequest.getCriteria(), EncoderContext.builder().build());
            writer.writeInt32("limit", removeRequest.isMulti() ? 0 : 1);
            writer.popMaxDocumentSize();
            writer.writeEndDocument();
            if (exceedsLimits(bsonOutput.getPosition() - commandStartPosition, i + 1)) {
                writer.reset();
                nextMessage = new DeleteCommandMessage(getWriteNamespace(),
                                                       isOrdered(), getWriteConcern(), deletes.subList(i, deletes.size()),
                                                       getSettings());
                break;
            }
        }
        writer.writeEndArray();
        return nextMessage;
    }
}
