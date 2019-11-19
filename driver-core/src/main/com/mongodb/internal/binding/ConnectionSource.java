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

import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.session.SessionContext;

/**
 * A source of connections to a single MongoDB server.
 *
 * @since 3.0
 */
public interface ConnectionSource extends ReferenceCounted {

    /**
     * Gets the current description of this source.
     *
     * @return the current details of the server state.
     */
    ServerDescription getServerDescription();

    /**
     * Gets the session context for this source
     *
     * @return the session context, which may not be null
     *
     * @since 3.6
     */
    SessionContext getSessionContext();

    /**
     * Gets a connection from this source.
     *
     * @return the connection
     */
    Connection getConnection();

    @Override
    ConnectionSource retain();
}
