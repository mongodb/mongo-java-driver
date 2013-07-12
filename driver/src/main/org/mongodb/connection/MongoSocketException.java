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

package org.mongodb.connection;

import org.mongodb.MongoException;

/**
 * Unchecked exception thrown when the driver gets an IOException from the underlying Socket, or reaches end of stream
 * prematurely.
 */
public abstract class MongoSocketException extends MongoException {
    private static final long serialVersionUID = -82458642694036972L;
    private final ServerAddress serverAddress;

    public MongoSocketException(final String message, final ServerAddress serverAddress, final Throwable t) {
        super(message, t);
        this.serverAddress = serverAddress;
    }

    public MongoSocketException(final String message, final ServerAddress serverAddress) {
        super(message);
        this.serverAddress = serverAddress;
    }

    public ServerAddress getServerAddress() {
        return serverAddress;
    }
}
