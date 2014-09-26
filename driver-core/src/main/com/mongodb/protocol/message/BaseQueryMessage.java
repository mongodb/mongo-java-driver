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

import org.bson.io.BsonOutput;

/**
 * Base class for OP_QUERY messages.
 *
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 * @since 3.0
 */
public abstract class BaseQueryMessage extends RequestMessage {

    private final int cursorFlags;
    private final int skip;
    private final int numberToReturn;

    /**
     * Construct an instance.
     *
     * @param collectionName the collection name
     * @param cursorFlags    the cursor flags
     * @param skip           the number of documents to skip
     * @param numberToReturn the number of documents to return
     * @param settings       the message settings
     */
    public BaseQueryMessage(final String collectionName, final int cursorFlags,
                            final int skip, final int numberToReturn, final MessageSettings settings) {
        super(collectionName, OpCode.OP_QUERY, settings);
        this.cursorFlags = cursorFlags;
        this.skip = skip;
        this.numberToReturn = numberToReturn;
    }

    /**
     * Write the query prologue to the given BSON output.
     *
     * @param bsonOutput the BSON output
     */
    protected void writeQueryPrologue(final BsonOutput bsonOutput) {
        bsonOutput.writeInt32(cursorFlags);
        bsonOutput.writeCString(getCollectionName());
        bsonOutput.writeInt32(skip);
        bsonOutput.writeInt32(numberToReturn);
    }
}
