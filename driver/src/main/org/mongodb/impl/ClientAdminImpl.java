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

package org.mongodb.impl;

import org.mongodb.ClientAdmin;
import org.mongodb.MongoConnection;
import org.mongodb.command.Ping;
import org.mongodb.result.CommandResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

/**
 * Contains the commands that can be run on MongoDB that do not require a database to be selected first.  These commands
 * can be accessed via MongoClient.
 */
class ClientAdminImpl implements ClientAdmin {
    private static final String ADMIN_DATABASE = "admin";
    private static final Ping PING_COMMAND = new Ping();

    private final DocumentSerializer documentSerializer;
    private final MongoConnection connection;

    ClientAdminImpl(final MongoConnection connection, final PrimitiveSerializers primitiveSerializers) {
        this.connection = connection;
        documentSerializer = new DocumentSerializer(primitiveSerializers);
    }

    //TODO: it's not clear from the documentation what the return type should be
    //http://docs.mongodb.org/manual/reference/command/ping/
    @Override
    public double ping() {
        final CommandResult pingResult = connection.executeCommand(ADMIN_DATABASE, PING_COMMAND, documentSerializer);

        return (Double) pingResult.getResponse().get("ok");
    }

}
