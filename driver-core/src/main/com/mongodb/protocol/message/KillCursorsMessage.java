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

import com.mongodb.ServerCursor;
import com.mongodb.protocol.KillCursor;
import org.bson.io.BsonOutput;

/**
 * An OP_KILL_CURSOR message.
 *
 * @mongodb.driver.manual meta-driver/latest/legacy/mongodb-wire-protocol/#op-kill-cursors OP_KILL_CURSOR
 * @since 3.0
 */
public class KillCursorsMessage extends RequestMessage {
    private final KillCursor killCursor;

    /**
     * Construct an instance.
     *
     * @param killCursor the list of cursors to kill
     */
    public KillCursorsMessage(final KillCursor killCursor) {
        super(OpCode.OP_KILL_CURSORS, MessageSettings.builder().build());
        this.killCursor = killCursor;
    }

    @Override
    protected RequestMessage encodeMessageBody(final BsonOutput bsonOutput, final int messageStartPosition) {
        writeKillCursorsPrologue(killCursor.getServerCursors().size(), bsonOutput);
        for (final ServerCursor cur : killCursor.getServerCursors()) {
            bsonOutput.writeInt64(cur.getId());
        }
        return null;
    }

    private void writeKillCursorsPrologue(final int numCursors, final BsonOutput bsonOutput) {
        bsonOutput.writeInt32(0); // reserved
        bsonOutput.writeInt32(numCursors);
    }
}
