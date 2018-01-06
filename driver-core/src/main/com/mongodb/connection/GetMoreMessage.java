/*
 * Copyright (c) 2008-2015 MongoDB, Inc.
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

import org.bson.io.BsonOutput;

/**
 * An OP_GET_MORE message.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-get-more OP_GET_MORE
 */
class GetMoreMessage extends LegacyMessage {
    private final long cursorId;
    private final int numberToReturn;

    GetMoreMessage(final String collectionName, final long cursorId, final int numberToReturn) {
        super(collectionName, OpCode.OP_GETMORE, MessageSettings.builder().build());
        this.cursorId = cursorId;
        this.numberToReturn = numberToReturn;
    }

    /**
     * Gets the cursor to get more from.
     *
     * @return the cursor id
     */
    public long getCursorId() {
        return cursorId;
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput) {
        writeGetMore(bsonOutput);
        return new EncodingMetadata(bsonOutput.getPosition());
    }

    private void writeGetMore(final BsonOutput buffer) {
        buffer.writeInt32(0);
        buffer.writeCString(getCollectionName());
        buffer.writeInt32(numberToReturn);
        buffer.writeInt64(cursorId);
    }

}
