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

package com.mongodb.client.jndi;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import java.util.Enumeration;
import java.util.Hashtable;

import static java.lang.String.format;

/**
 * An ObjectFactory for MongoClient instances.
 *
 * @since 3.3
 */
public class MongoClientFactory implements ObjectFactory {

    private static final Logger LOGGER = Loggers.getLogger("client.jndi");

    private static final String CONNECTION_STRING = "connectionString";

    /**
     * This implementation will create instances of {@link MongoClient} based on a connection string conforming to the format specified in
     * {@link MongoClientURI}.
     * <p>The connection string is specified in one of two ways:</p>
     * <ul>
     * <li>As the {@code String} value of a property in the {@code environment} parameter with a key of {@code "connectionString"}</li>
     * <li>As the {@code String} value of a {@link RefAddr} with type {@code "connectionString"} in an {@code obj} parameter
     * of type {@link Reference}</li>
     * </ul>
     *
     * Specification of the connection string in the {@code environment} parameter takes precedence over specification in the {@code obj}
     * parameter.  The {@code name} and {@code nameCtx} parameters are ignored.
     *
     * If a non-empty connection string is not specified in either of these two ways, a {@link MongoException} is thrown.
     * @return an instance of {@link MongoClient} based on the specified connection string
     * @throws MongoException
     *
     * Note: Not all options that can be specified via {@link com.mongodb.MongoClientOptions} can be specified via the connection string.
     */
    @Override
    public Object getObjectInstance(final Object obj, final Name name, final Context nameCtx, final Hashtable<?, ?> environment)
            throws Exception {

        // Some app servers, e.g. Wildfly, use the environment to pass location information to an ObjectFactory
        String connectionString = null;

        if (environment.get(CONNECTION_STRING) instanceof String) {
            connectionString = (String) environment.get(CONNECTION_STRING);
        }

        if (connectionString == null || connectionString.isEmpty()) {
            LOGGER.debug(format("No '%s' property in environment.  Casting 'obj' to java.naming.Reference to look for a "
                                        + "javax.naming.RefAddr with type equal to '%s'", CONNECTION_STRING, CONNECTION_STRING));

            // Some app servers, e.g. Tomcat, pass obj as an instance of javax.naming.Reference and pass location information in a
            // javax.naming.RefAddr
            if (obj instanceof Reference) {
                Enumeration<RefAddr> props = ((Reference) obj).getAll();

                while (props.hasMoreElements()) {
                    RefAddr addr = props.nextElement();
                    if (addr != null) {
                        if (CONNECTION_STRING.equals(addr.getType())) {
                            if (addr.getContent() instanceof String) {
                                connectionString = (String) addr.getContent();
                                break;
                            }
                        }
                    }
                }
            }
        }

        if (connectionString == null || connectionString.isEmpty()) {
            throw new MongoException(format("Could not locate '%s' in either environment or obj", CONNECTION_STRING));
        }

        MongoClientURI uri = new MongoClientURI(connectionString);

        return new MongoClient(uri);
    }
}
