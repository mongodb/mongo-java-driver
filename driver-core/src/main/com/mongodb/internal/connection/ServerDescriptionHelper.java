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

import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.lang.Nullable;

import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.connection.ServerType.UNKNOWN;

final class ServerDescriptionHelper {
    static ServerDescription unknownConnectingServerDescription(final ServerId serverId, @Nullable final Throwable cause) {
        ServerDescription.Builder result = ServerDescription.builder()
                .type(UNKNOWN)
                .state(CONNECTING)
                .address(serverId.getAddress());
        TopologyVersionHelper.topologyVersion(cause)
                .ifPresent(result::topologyVersion);
        if (cause != null) {
            result.exception(cause);
        }
        return result.build();
    }

    private ServerDescriptionHelper() {
        throw new AssertionError();
    }
}
