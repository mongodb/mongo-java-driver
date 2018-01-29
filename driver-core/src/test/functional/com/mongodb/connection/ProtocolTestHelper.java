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

package com.mongodb.connection;

import com.mongodb.ClusterFixture;
import com.mongodb.async.FutureResultCallback;

import static java.util.concurrent.TimeUnit.SECONDS;

final class ProtocolTestHelper {
    public static <T> T execute(final LegacyProtocol<T> protocol, final InternalConnection connection, final boolean async)
            throws Throwable{
        if (async) {
            final FutureResultCallback<T> futureResultCallback = new FutureResultCallback<T>();
            protocol.executeAsync(connection, futureResultCallback);
            return futureResultCallback.get(ClusterFixture.TIMEOUT, SECONDS);
        } else {
            return protocol.execute(connection);
        }
    }

    public static <T> T execute(final CommandProtocol<T> protocol, final InternalConnection connection, final boolean async)
            throws Throwable{
        if (async) {
            final FutureResultCallback<T> futureResultCallback = new FutureResultCallback<T>();
            protocol.executeAsync(connection, futureResultCallback);
            return futureResultCallback.get(ClusterFixture.TIMEOUT, SECONDS);
        } else {
            return protocol.execute(connection);
        }
    }

    private ProtocolTestHelper() {}
}
