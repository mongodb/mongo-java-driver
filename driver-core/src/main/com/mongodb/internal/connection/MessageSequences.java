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

/**
 * Zero or more identifiable sequences contained in the {@code OP_MSG} section with payload type 1.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 * @see <a href="https://github.com/mongodb/specifications/blob/master/source/message/OP_MSG.md">OP_MSG</a>
 */
public abstract class MessageSequences {
    public static final class EmptyMessageSequences extends MessageSequences {
        public static final EmptyMessageSequences INSTANCE = new EmptyMessageSequences();

        private EmptyMessageSequences() {
        }
    }
}
