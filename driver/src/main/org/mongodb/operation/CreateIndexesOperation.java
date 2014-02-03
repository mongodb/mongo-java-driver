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
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoDuplicateKeyException;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class CreateIndexesOperation extends BaseOperation<Void> {
    private final List<Index> indexes;
    private final MongoNamespace namespace;

    public CreateIndexesOperation(final List<Index> indexes, final MongoNamespace namespace, final BufferProvider bufferProvider,
                                  final Session session, final boolean closeSession) {
        super(bufferProvider, session, closeSession);
        this.indexes = indexes;
        this.namespace = namespace;
    }

    @Override
    public Void execute() {
        if (getPrimaryServerConnectionProvider().getServerDescription().getVersion().compareTo(new ServerVersion(asList(2, 5, 5))) >= 0) {
            try {
                executeCommandBasedProtocol();
            } catch (MongoCommandFailureException e) {
                if (e.getErrorCode() == 11000) {
                    throw new MongoDuplicateKeyException(e.getErrorCode(), e.getErrorMessage(), e.getCommandResult());
                } else {
                    throw e;
                }
            }
        } else {
            executeCollectionBasedProtocol();
        }
        return null;

    }

    @SuppressWarnings("unchecked")
    private void executeCollectionBasedProtocol() {
        MongoNamespace systemIndexes = new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
        for (Index index : indexes) {
            new InsertOperation<Document>(systemIndexes, true, WriteConcern.ACKNOWLEDGED,
                                          asList(new InsertRequest<Document>(toDocument(index))),
                                          new DocumentCodec(), getBufferProvider(), getSession(), false).execute();
        }
    }


    private void executeCommandBasedProtocol() {
        Document command = new Document("createIndexes", namespace.getCollectionName());
        List<Document> list = new ArrayList<Document>();
        for (Index index : indexes) {
            list.add(toDocument(index));
        }
        command.append("indexes", list);


        CommandProtocol commandProtocol = new CommandProtocol(namespace.getDatabaseName(), command,
                                                              new DocumentCodec(),
                                                              new DocumentCodec(), getBufferProvider(),
                                                              getPrimaryServerConnectionProvider().getServerDescription(),
                                                              getPrimaryServerConnectionProvider().getConnection(), true);
        commandProtocol.execute();

    }
    
    public Document toDocument(final Index index) {
        Document indexDetails = new Document();
        indexDetails.append("name", index.getName());
        indexDetails.append("key", index.getKeys());
        if (index.isUnique()) {
            indexDetails.append("unique", index.isUnique());
        }
        if (index.isSparse()) {
            indexDetails.append("sparse", index.isSparse());
        }
        if (index.isDropDups()) {
            indexDetails.append("dropDups", index.isDropDups());
        }
        if (index.isBackground()) {
            indexDetails.append("background", index.isBackground());
        }
        if (index.getExpireAfterSeconds() != -1) {
            indexDetails.append("expireAfterSeconds", index.getExpireAfterSeconds());
        }
        indexDetails.putAll(index.getExtra());
        indexDetails.put("ns", namespace.toString());

        return indexDetails;
    }
    
}
