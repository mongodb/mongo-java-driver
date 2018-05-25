/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection;

import org.bson.io.BsonOutput;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An OP_KILL_CURSOR message.
 *
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-kill-cursors OP_KILL_CURSOR
 */
class KillCursorsMessage extends LegacyMessage {
    private final List<Long> cursors;

    KillCursorsMessage(final List<Long> cursors) {
        super(OpCode.OP_KILL_CURSORS, MessageSettings.builder().build());
        this.cursors = notNull("cursors", cursors);
    }

    @Override
    protected EncodingMetadata encodeMessageBodyWithMetadata(final BsonOutput bsonOutput) {
        writeKillCursorsPrologue(cursors.size(), bsonOutput);
        for (final Long cur : cursors) {
            bsonOutput.writeInt64(cur);
        }
        return new EncodingMetadata(bsonOutput.getPosition());
    }

    private void writeKillCursorsPrologue(final int numCursors, final BsonOutput bsonOutput) {
        bsonOutput.writeInt32(0); // reserved
        bsonOutput.writeInt32(numCursors);
    }
}
