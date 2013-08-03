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

public class KillCursorsMessage extends RequestMessage {
    private final KillCursor killCursor;

    public KillCursorsMessage(final KillCursor killCursor, final MessageSettings settings) {
        super(OpCode.OP_KILL_CURSORS, settings);
        this.killCursor = killCursor;
    }

    @Override
    protected RequestMessage encodeMessageBody(final OutputBuffer buffer, final int messageStartPosition) {
        writeKillCursorsPrologue(1, buffer);
        buffer.writeLong(killCursor.getServerCursor().getId());
        return null;
    }

    private void writeKillCursorsPrologue(final int numCursors, final OutputBuffer buffer) {
        buffer.writeInt(0); // reserved
        buffer.writeInt(numCursors);
    }
}
