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
    private final MongoGetMore getMore;

    public MongoGetMoreMessage(final String collectionName, final MongoGetMore getMore) {
        super(collectionName, OpCode.OP_GETMORE);
        this.getMore = getMore;
    }

    public long getCursorId() {
        return getMore.getServerCursor().getId();
    }

    @Override
    protected MongoRequestMessage encodeMessageBody(final ChannelAwareOutputBuffer buffer, final int messageStartPosition) {
        writeGetMore(buffer);
        return null;
    }

    private void writeGetMore(final ChannelAwareOutputBuffer buffer) {
        buffer.writeInt(0);
        buffer.writeCString(getCollectionName());
        buffer.writeInt(getMore.getNumberToReturn());
        buffer.writeLong(getMore.getServerCursor().getId());
    }

}
