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

import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.Callable;

// Implementation of
// https://github.com/mongodb/specifications/blob/master/source/connection-monitoring-and-pooling/connection-monitoring-and-pooling.rst
// specification tests
@RunWith(Parameterized.class)
public class ConnectionPoolTest extends AbstractConnectionPoolTest {
    private static final Logger LOGGER = Loggers.getLogger(ConnectionPoolTest.class.getSimpleName());

    public ConnectionPoolTest(final String fileName, final String description, final BsonDocument definition, final boolean skipTest) {
        super(fileName, description, definition, skipTest);
    }

    @Override
    protected Callable<Exception> createCallable(final BsonDocument operation) {
        String name = operation.getString("name").getValue();
        if (name.equals("checkOut")) {
            return () -> {
                try {
                    InternalConnection connection = getPool().get(new OperationContext());
                    if (operation.containsKey("label")) {
                        getConnectionMap().put(operation.getString("label").getValue(), connection);
                    }
                    return null;
                } catch (Exception e) {
                    LOGGER.error("", e);
                    return e;
                }
            };
        } else if (name.equals("checkIn")) {
            return () -> {
                try {
                    InternalConnection connection = getConnectionMap().get(operation.getString("connection").getValue());
                    connection.close();
                    return null;
                } catch (Exception e) {
                    return e;
                }
            };
        } else {
            throw new UnsupportedOperationException("Operation " + name + " not supported");
        }
    }

    @Override
    protected StreamFactory createStreamFactory(final SocketSettings socketSettings, final SslSettings sslSettings) {
        return new SocketStreamFactory(socketSettings, sslSettings);
    }
}
