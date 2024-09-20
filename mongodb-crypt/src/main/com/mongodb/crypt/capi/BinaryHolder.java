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
 *
 */

package com.mongodb.crypt.capi;

import com.mongodb.crypt.capi.CAPI.mongocrypt_binary_t;

import static com.mongodb.crypt.capi.CAPI.mongocrypt_binary_destroy;

// Wrap JNA memory and a mongocrypt_binary_t that references that memory, in order to ensure that the JNA Memory is not GC'd before the
// mongocrypt_binary_t is destroyed
class BinaryHolder implements AutoCloseable {

    private final DisposableMemory memory;
    private final mongocrypt_binary_t binary;

    BinaryHolder(final DisposableMemory memory, final mongocrypt_binary_t binary) {
        this.memory = memory;
        this.binary = binary;
    }

    mongocrypt_binary_t getBinary() {
        return binary;
    }

    @Override
    public void close() {
        mongocrypt_binary_destroy(binary);
        memory.dispose();
    }
}
