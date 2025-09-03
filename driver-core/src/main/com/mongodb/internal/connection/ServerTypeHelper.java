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

import com.mongodb.connection.ServerType;

final class ServerTypeHelper {
    static boolean isDataBearing(final ServerType serverType) {
        switch (serverType) {
            case STANDALONE:
            case REPLICA_SET_PRIMARY:
            case REPLICA_SET_SECONDARY:
            case SHARD_ROUTER:
            case LOAD_BALANCER: {
                return true;
            }
            case REPLICA_SET_ARBITER:
            case REPLICA_SET_OTHER:
            case REPLICA_SET_GHOST:
            case UNKNOWN: {
                return false;
            }
            default: {
                throw new AssertionError();
            }
        }
    }

    private ServerTypeHelper() {
        throw new AssertionError();
    }
}
