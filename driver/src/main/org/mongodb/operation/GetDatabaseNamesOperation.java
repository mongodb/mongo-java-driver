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

package org.mongodb.operation;

import org.mongodb.Codec;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * Execute this operation to return a List of Strings of the names of all the databases for the current MongoDB instance.
 */
public class GetDatabaseNamesOperation extends BaseOperation<List<String>> {
    private final Codec<Document> commandCodec = new DocumentCodec();

    /**
     * Set up the Operation with all the basic information required to get the database names from MongoDB
     *
     * @param bufferProvider the BufferProvider to use when reading or writing to the network
     * @param session        the current Session, which will give access to a connection to the MongoDB instance
     * @param closeSession   true if the session should be closed at the end of the execute method
     */
    public GetDatabaseNamesOperation(final BufferProvider bufferProvider, final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
    }

    /**
     * Executing this will return a list of all the databases names in the MongoDB instance.
     *
     * @return a List of Strings of the names of all the databases in the MongoDB instance
     */
    @Override
    public List<String> execute() {
        ServerConnectionProvider provider = getPrimaryServerConnectionProvider();
        CommandResult listDatabasesResult = new CommandProtocol("admin", new Document("listDatabases", 1), commandCodec, commandCodec,
                                                                getBufferProvider(), provider.getServerDescription(),
                                                                provider.getConnection(), true).execute();

        @SuppressWarnings("unchecked")
        List<Document> databases = (List<Document>) listDatabasesResult.getResponse().get("databases");

        List<String> databaseNames = new ArrayList<String>();
        for (final Document database : databases) {
            databaseNames.add(database.get("name", String.class));
        }
        return unmodifiableList(databaseNames);
    }

}
