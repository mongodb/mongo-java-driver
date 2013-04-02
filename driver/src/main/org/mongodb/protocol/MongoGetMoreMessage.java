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

import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.operation.MongoGetMore;

public class MongoGetMoreMessage extends MongoRequestMessage {
    private final MongoGetMore mongoGetMore;

    public MongoGetMoreMessage(final String collectionName, final MongoGetMore mongoGetMore) {
        super(collectionName, OpCode.OP_GETMORE);
        this.mongoGetMore = mongoGetMore;
    }

    public long getCursorId() {
        return mongoGetMore.getServerCursor().getId();
    }

    @Override
    protected void serializeMessageBody(final ChannelAwareOutputBuffer buffer) {
        writeGetMore(buffer);
    }

    private void writeGetMore(final ChannelAwareOutputBuffer buffer) {
        buffer.writeInt(0);
        buffer.writeCString(getCollectionName());
        buffer.writeInt(mongoGetMore.getBatchSize());
        buffer.writeLong(mongoGetMore.getServerCursor().getId());
    }
}
