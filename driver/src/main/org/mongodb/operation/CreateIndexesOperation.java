/*
 * Copyright (c) 2008 - 2014 10gen, Inc. <http://10gen.com>
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

import org.mongodb.Document;
import org.mongodb.Index;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ClusterDescription;
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class CreateIndexesOperation extends BaseOperation<Void> {
    private final List<Index> indexes;
    private final MongoNamespace namespace;
    private final ClusterDescription clusterDescription;

    public CreateIndexesOperation(final List<Index> indexes, final MongoNamespace namespace, final ClusterDescription clusterDescription,
                                  final Session session, final boolean closeSession, final BufferProvider bufferProvider) {
        super(bufferProvider, session, closeSession);
        this.indexes = indexes;
        this.namespace = namespace;
        this.clusterDescription = clusterDescription;
    }

    @Override
    public Void execute() {
        if (getPrimaryServerConnectionProvider().getServerDescription().getVersion().compareTo(new ServerVersion(asList(2, 5, 5))) >= 0) {
            executeCommandBasedProtocol();
        } else {
            executeCollectionBasedProtocol();
        }
        return null;

    }

    @SuppressWarnings("unchecked")
    private void executeCollectionBasedProtocol() {
        MongoNamespace systemIndexes = new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
        for (Index index : indexes) {
            Document document = index.toDocument();
            document.put("ns", namespace.toString());
            new InsertOperation<Document>(systemIndexes, true, WriteConcern.ACKNOWLEDGED,
                                          asList(new InsertRequest<Document>(document)),
                                          new DocumentCodec(), getBufferProvider(), getSession(), false).execute();
        }
    }


    private void executeCommandBasedProtocol() {
        Document command = new Document("createIndexes", namespace.getCollectionName());
        List<Document> list = new ArrayList<Document>();
        for (Index index : indexes) {
            Document document = index.toDocument();
            document.put("ns", namespace.toString());
            list.add(document);
        }
        command.append("indexes", list);
//        new CommandOperation(namespace.getDatabaseName(), command, ReadPreference.primary(), new DocumentCodec(),
//                             new DocumentCodec(), clusterDescription, getBufferProvider(), getSession(), false)
//            .execute();
        
        
        CommandProtocol commandProtocol = new CommandProtocol(namespace.getDatabaseName(), command,
                                                              new DocumentCodec(),
                                                              new DocumentCodec(), getBufferProvider(),
                                                              getPrimaryServerConnectionProvider().getServerDescription(),
                                                              getPrimaryServerConnectionProvider().getConnection(), true);
        commandProtocol.execute();
        
    }
}
