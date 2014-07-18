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

import org.bson.io.OutputBuffer;
import org.mongodb.operation.QueryFlag;

import java.util.EnumSet;

public abstract class BaseQueryMessage extends RequestMessage {

    private final EnumSet<QueryFlag> queryFlags;
    private final int skip;
    private final int numberToReturn;

    public BaseQueryMessage(final String collectionName, final EnumSet<QueryFlag> queryFlags,
                            final int skip, final int numberToReturn, final MessageSettings settings) {
        super(collectionName, OpCode.OP_QUERY, settings);
        this.queryFlags = queryFlags;
        this.skip = skip;
        this.numberToReturn = numberToReturn;
    }

    protected void writeQueryPrologue(final OutputBuffer buffer) {
        buffer.writeInt(QueryFlag.fromSet(queryFlags));
        buffer.writeCString(getCollectionName());
        buffer.writeInt(skip);
        buffer.writeInt(numberToReturn);
    }
}
