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

package com.mongodb.internal.binding;

import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

/**
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class StaticBindingContext implements BindingContext {
    private final SessionContext sessionContext;
    private final ServerApi serverApi;
    private final RequestContext requestContext;
    private final OperationContext operationContext;

    public StaticBindingContext(final SessionContext sessionContext, @Nullable final ServerApi serverApi,
            final RequestContext requestContext, final OperationContext operationContext) {
        this.sessionContext = sessionContext;
        this.serverApi = serverApi;
        this.requestContext = requestContext;
        this.operationContext = operationContext;
    }

    @Override
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    @Nullable
    @Override
    public ServerApi getServerApi() {
        return serverApi;
    }

    @Override
    public RequestContext getRequestContext() {
        return requestContext;
    }

    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }
}
