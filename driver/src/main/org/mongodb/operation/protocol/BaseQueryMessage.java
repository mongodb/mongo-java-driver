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
import org.mongodb.operation.Query;
import org.mongodb.operation.QueryFlag;

public abstract class BaseQueryMessage extends RequestMessage {

    public BaseQueryMessage(final String collectionName, final MessageSettings settings) {
        super(collectionName, OpCode.OP_QUERY, settings);
    }

    protected void writeQueryPrologue(final Query query, final OutputBuffer buffer) {
        buffer.writeInt(QueryFlag.fromSet(query.getOptions().getFlags()));
        buffer.writeCString(getCollectionName());

        buffer.writeInt(query.getSkip());
        buffer.writeInt(query.getNumberToReturn());
    }
}
