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

import org.bson.io.OutputBuffer;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.operation.BaseUpdate;
import org.mongodb.operation.Update;

import java.util.List;

public class UpdateMessage extends BaseUpdateMessage {
    private List<Update> updates;

    public UpdateMessage(final String collectionName, final List<Update> updates, final Encoder<Document> encoder,
                         final MessageSettings settings) {
        super(collectionName, OpCode.OP_UPDATE, encoder, settings);
        this.updates = updates;
    }

    @Override
    protected RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        writeBaseUpdate(buffer);
        addDocument(updates.get(0).getUpdateOperations(), getBaseEncoder(), buffer);
        if (updates.size() == 1) {
            return null;
        }
        else {
            return new UpdateMessage(getCollectionName(), updates.subList(1, updates.size()), getBaseEncoder(), getSettings());
        }
    }

    @Override
    protected BaseUpdate getUpdateBase() {
        return updates.get(0);
    }
}
