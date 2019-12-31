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

enum OpCode {
    OP_REPLY(1),
    OP_UPDATE(2001),
    OP_INSERT(2002),
    OP_QUERY(2004),
    OP_GETMORE(2005),
    OP_DELETE(2006),
    OP_KILL_CURSORS(2007),
    OP_COMPRESSED(2012),
    OP_MSG(2013);

    OpCode(final int value) {
        this.value = value;
    }

    private final int value;

    public int getValue() {
        return value;
    }
}
