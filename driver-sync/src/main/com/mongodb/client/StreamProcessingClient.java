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

package com.mongodb.client;

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Reason;
import com.mongodb.annotations.Sealed;
import com.mongodb.annotations.ThreadSafe;

import java.io.Closeable;

/**
 * A client for connecting to an Atlas Stream Processing workspace.
 *
 * <p>Workspace connection strings match the pattern
 * {@code mongodb://[username:password@]atlas-stream-*.*a.query.mongodb*.net/}.
 * TLS, {@code loadBalanced=true}, and {@code authSource=admin} are applied automatically.</p>
 *
 * <p>Use {@link StreamProcessingClients} to create an instance.</p>
 *
 * @since 5.5
 */
@Alpha(Reason.CLIENT)
@Sealed
@ThreadSafe
public interface StreamProcessingClient extends Closeable {

    /**
     * Closes this client, releasing all underlying resources.
     */
    @Override
    void close();
}
