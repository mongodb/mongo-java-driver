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
import com.mongodb.connection.ServerId;
import com.mongodb.lang.NonNull;
import org.bson.types.ObjectId;

@ThreadSafe
interface InternalConnectionFactory {
    default InternalConnection create(ServerId serverId) {
        return create(serverId, new ConnectionGenerationSupplier() {
            @Override
            public int getGeneration() {
                return 0;
            }

            @Override
            public int getGeneration(@NonNull final ObjectId serviceId) {
                return 0;
            }
        });
    }

    InternalConnection create(ServerId serverId, ConnectionGenerationSupplier connectionGenerationSupplier);
}
