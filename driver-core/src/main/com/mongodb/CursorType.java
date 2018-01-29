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

package com.mongodb;

/**
 * An enumeration of cursor types.
 *
 * @since 3.0
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#op-query OP_QUERY
 */
public enum CursorType {
    /**
     * A non-tailable cursor. This is sufficient for a vast majority of uses.
     */
    NonTailable {
        @Override
        public boolean isTailable() {
            return false;
        }
    },

    /**
     * Tailable means the cursor is not closed when the last data is retrieved. Rather, the cursor marks the final object's position. You
     * can resume using the cursor later, from where it was located, if more data were received. Like any "latent cursor",
     * the cursor may become invalid at some point - for example if the final object it references were deleted.
     */
    Tailable {
        @Override
        public boolean isTailable() {
            return true;
        }
    },

    /**
     *  A tailable cursor with a built-in server sleep before returning an empty batch. In most cases this is preferred type of tailable
     *  cursor, as it is less resource intensive.
     */
    TailableAwait {
        @Override
        public boolean isTailable() {
            return true;
        }
    };

    /**
     * True if the cursor type is tailable.
     *
     * @return true if the cursor type is tailable
     */
    public abstract boolean isTailable();
}
