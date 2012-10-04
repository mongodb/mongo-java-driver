/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.protocol;

import org.bson.io.OutputBuffer;

public class MongoGetMoreMessage extends MongoRequestMessage {
    public MongoGetMoreMessage(String collectionName, long cursorId, int batchSize,
                               OutputBuffer buffer) {
        super(collectionName, OpCode.OP_GETMORE, buffer);
        writeGetMore(cursorId, batchSize);
    }

    private void writeGetMore(final long cursorId, final int batchSize) {
        buffer.writeInt(0);
        buffer.writeCString(collectionName);
        buffer.writeInt(batchSize);
        buffer.writeLong(cursorId);
    }

}
