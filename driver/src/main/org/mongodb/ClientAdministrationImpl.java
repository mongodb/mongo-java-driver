/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package org.mongodb;

import org.bson.codecs.BsonDocumentCodec;
import org.bson.types.BsonDocument;
import org.bson.types.BsonDouble;
import org.bson.types.BsonInt32;
import org.mongodb.operation.CommandReadOperation;
import org.mongodb.operation.GetDatabaseNamesOperation;

import java.util.List;

import static org.mongodb.ReadPreference.primary;

/**
 * Contains the commands that can be run on MongoDB that do not require a database to be selected first.  These commands can be accessed via
 * MongoClient.
 */
class ClientAdministrationImpl implements ClientAdministration {
    private static final String ADMIN_DATABASE = "admin";
    private static final BsonDocument PING_COMMAND = new BsonDocument("ping", new BsonInt32(1));

    private final MongoClientImpl client;

    ClientAdministrationImpl(final MongoClientImpl client) {
        this.client = client;
    }

    //TODO: it's not clear from the documentation what the return type should be
    //http://docs.mongodb.org/manual/reference/command/ping/
    @Override
    public double ping() {
        CommandResult pingResult = client.execute(new CommandReadOperation(ADMIN_DATABASE, PING_COMMAND,
                                                                           new BsonDocumentCodec()),
                                                  client.getOptions().getReadPreference()
                                                 );

        return ((BsonDouble) pingResult.getResponse().get("ok")).getValue();
    }

    @Override
    public List<String> getDatabaseNames() {
        return client.execute(new GetDatabaseNamesOperation(), primary());
    }
}
