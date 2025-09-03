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

import com.mongodb.annotations.ThreadSafe;
import org.bson.ByteBuf;

/**
 * A provider of instances of ByteBuf.
 */
@ThreadSafe
public interface BufferProvider {
    /**
     * Gets a buffer with the givens capacity.
     *
     * @param size the size required for the buffer
     * @return a ByteBuf with the given size, which is now owned by the caller and must be released.
     */
    ByteBuf getBuffer(int size);
}
