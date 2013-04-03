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

package org.mongodb.protocol;

import org.mongodb.Document;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.operation.MongoUpdateBase;
import org.mongodb.Encoder;

public class MongoUpdateMessage extends MongoUpdateBaseMessage {
    private MongoUpdate update;

    public MongoUpdateMessage(final String fullName, final MongoUpdate update, final Encoder<Document> encoder) {
        super(fullName, OpCode.OP_UPDATE, encoder);
        this.update = update;
    }

    @Override
    protected void encodeMessageBody(final ChannelAwareOutputBuffer buffer) {
        writeBaseUpdate(buffer);
        addDocument(update.getUpdateOperations(), getBaseEncoder(), buffer);
    }

    @Override
    protected MongoUpdateBase getUpdateBase() {
        return update;
    }
}
