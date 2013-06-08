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

public class UpdateMessage extends BaseUpdateMessage {
    private Update update;

    public UpdateMessage(final String fullName, final Update update, final Encoder<Document> encoder,
                         final MessageSettings settings) {
        super(fullName, OpCode.OP_UPDATE, encoder, settings);
        this.update = update;
    }

    @Override
    protected RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        writeBaseUpdate(buffer);
        addDocument(update.getUpdateOperations(), getBaseEncoder(), buffer);
        return null;
    }

    @Override
    protected BaseUpdate getUpdateBase() {
        return update;
    }
}
