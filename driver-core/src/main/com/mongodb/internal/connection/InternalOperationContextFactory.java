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

import com.mongodb.ServerApi;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.lang.Nullable;

import static com.mongodb.internal.connection.OperationContext.simpleOperationContext;

public final class InternalOperationContextFactory {

    private final TimeoutSettings timeoutSettings;
    @Nullable
    private final ServerApi serverApi;

    public InternalOperationContextFactory(final TimeoutSettings timeoutSettings, @Nullable final ServerApi serverApi) {
        this.timeoutSettings = timeoutSettings;
        this.serverApi = serverApi;
    }

    /**
     * @return a simple operation context without timeoutMS
     */
    OperationContext create() {
        return simpleOperationContext(timeoutSettings.connectionOnly(), serverApi);
    }

    /**
     * @return a simple operation context with timeoutMS if set at the MongoClientSettings level
     */

    OperationContext createMaintenanceContext() {
        return create().withTimeoutContext(TimeoutContext.createMaintenanceTimeoutContext(timeoutSettings));
    }
}
