/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson.io;

/**
 * Represents a bookmark that can be used to reset a {@link BsonInput} to its state at the time the mark was created.
 *
 * @see BsonInput#getMark(int)
 *
 * @since 3.7
 */
public interface BsonInputMark {
    /**
     * Reset the {@link BsonInput} to its state at the time the mark was created.
     */
    void reset();
}
