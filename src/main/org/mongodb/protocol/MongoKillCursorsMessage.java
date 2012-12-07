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
import org.mongodb.operation.MongoKillCursor;

public class MongoKillCursorsMessage extends MongoRequestMessage {
    public MongoKillCursorsMessage(OutputBuffer buffer, MongoKillCursor killCursor) {
        super(OpCode.OP_KILL_CURSORS, buffer);
        writeKillCursorsPrologue(killCursor.getCursorIds().size());
        for (long curCursorId : killCursor.getCursorIds()) {
            buffer.writeLong(curCursorId);
        }
        backpatchMessageLength();
    }

    private void writeKillCursorsPrologue(final int numCursors) {
        buffer.writeInt(0); // reserved
        buffer.writeInt(numCursors);
    }
}
