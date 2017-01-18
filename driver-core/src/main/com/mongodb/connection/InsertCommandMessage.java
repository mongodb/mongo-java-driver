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

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.internal.validator.CollectibleDocumentFieldNameValidator;
import com.mongodb.internal.validator.MappedFieldNameValidator;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.EncoderContext;
import org.bson.io.BsonOutput;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An insert command message.
 *
 * @mongodb.driver.manual reference/command/insert/#dbcmd.insert Insert Command
 */
class InsertCommandMessage extends BaseWriteCommandMessage {
    private final List<InsertRequest> insertRequestList;

    InsertCommandMessage(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                         final Boolean bypassDocumentValidation, final MessageSettings settings,
                         final List<InsertRequest> insertRequestList) {
        super(namespace, ordered, writeConcern, bypassDocumentValidation, settings);
        this.insertRequestList = notNull("insertRequestList", insertRequestList);
    }

    @Override
    public int getItemCount() {
        return insertRequestList.size();
    }

    @Override
    protected FieldNameValidator getFieldNameValidator() {
        Map<String, FieldNameValidator> map = new HashMap<String, FieldNameValidator>();
        map.put("documents", new CollectibleDocumentFieldNameValidator());
        return new MappedFieldNameValidator(new NoOpFieldNameValidator(), map);
    }

    /**
     * Gets the list of insert requests.
     *
     * @return the non-null list of insert requests
     */
    public List<InsertRequest> getRequests() {
        return Collections.unmodifiableList(insertRequestList);
    }

    /**
     * Gets the command name, which is "insert".
     *
     * @return the command name
     */
    protected String getCommandName() {
        return "insert";
    }

    protected InsertCommandMessage writeTheWrites(final BsonOutput bsonOutput, final int commandStartPosition,
                                                  final BsonBinaryWriter writer) {
        InsertCommandMessage nextMessage = null;
        writer.writeStartArray("documents");
        writer.pushMaxDocumentSize(getSettings().getMaxDocumentSize());
        for (int i = 0; i < insertRequestList.size(); i++) {
            writer.mark();
            BsonDocument document = insertRequestList.get(i).getDocument();
            getCodec(document).encode(writer, document, EncoderContext.builder().isEncodingCollectibleDocument(true).build());
            if (exceedsLimits(bsonOutput.getPosition() - commandStartPosition, i + 1)) {
                writer.reset();
                nextMessage = new InsertCommandMessage(getWriteNamespace(), isOrdered(), getWriteConcern(), getBypassDocumentValidation(),
                        getSettings(), insertRequestList.subList(i, insertRequestList.size()));
                break;
            }
        }
        writer.popMaxDocumentSize();
        writer.writeEndArray();
        return nextMessage;
    }
}
