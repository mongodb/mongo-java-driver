/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.connection.BufferProvider;
import org.mongodb.session.PrimaryServerSelector;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import static org.mongodb.assertions.Assertions.notNull;

/**
 * Contains the common fields required by simple operations.
 *
 * @param <T> the return type for the execute method
 */
public abstract class BaseOperation<T> implements Operation<T> {
    private final BufferProvider bufferProvider;
    private final Session session;
    private final boolean closeSession;

    /**
     * The constructor of this abstract class takes the fields that are required by all basic operations.
     *
     * @param bufferProvider the BufferProvider to use when reading or writing to the network
     * @param session        the current Session, which will give access to a connection to the MongoDB instance
     * @param closeSession   true if the session should be closed at the end of the execute method
     */
    public BaseOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession) {
        this.bufferProvider = notNull("bufferProvider", bufferProvider);
        this.session = notNull("session", session);
        this.closeSession = closeSession;
    }

    /**
     * Getter is largely used inside the execute method of subclasses.  This is a public method so that users can implement their own
     * Operations that inherit from this abstract class.
     *
     * @return the bufferProvider injected via the constructor
     */
    public BufferProvider getBufferProvider() {
        return bufferProvider;
    }

    /**
     * Getter is largely used inside the execute method of subclasses.  This is a public method so that users can implement their own
     * Operations that inherit from this abstract class.
     *
     * @return the session injected via the constructor
     */
    public Session getSession() {
        return session;
    }

    /**
     * Getter is largely used inside the execute method of subclasses.  This is a public method so that users can implement their own
     * Operations that inherit from this abstract class.
     *
     * @return true if this operation should close the session at the end of the execute method
     */
    public boolean isCloseSession() {
        return closeSession;
    }

    /**
     * Use this method to get a ServerConnectionProvider that doesn't rely on specified read preferences.  Used by Operations like commands
     * which always run against the primary.
     *
     * @return a ServerConnectionProvider initialise with a PrimaryServerSelector
     */
    protected ServerConnectionProvider getPrimaryServerConnectionProvider() {
        return getSession().createServerConnectionProvider(new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
    }
}
