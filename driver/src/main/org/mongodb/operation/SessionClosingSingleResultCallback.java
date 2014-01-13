/*
 * Copyright (c) 2008 MongoDB, Inc.
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

package org.mongodb.operation;

import org.mongodb.MongoException;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.session.Session;

class SessionClosingSingleResultCallback<T> implements SingleResultCallback<T> {
    private final SingleResultFuture<T> retVal;
    private final Session session;
    private final boolean closeSession;

    public SessionClosingSingleResultCallback(final SingleResultFuture<T> retVal, final Session session, final boolean closeSession) {
        this.retVal = retVal;
        this.session = session;
        this.closeSession = closeSession;
    }

    @Override
    public void onResult(final T result, final MongoException e) {
        if (closeSession) {
            session.close();
        }
        retVal.init(result, e);
    }
}
