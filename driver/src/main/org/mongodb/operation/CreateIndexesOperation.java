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

import org.mongodb.Document;
import org.mongodb.Index;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoDuplicateKeyException;
import org.mongodb.MongoNamespace;
import org.mongodb.WriteConcern;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.ServerVersion;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.InsertProtocol;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.mongodb.operation.OperationHelper.executeProtocol;
import static org.mongodb.operation.OperationHelper.getPrimaryConnectionProvider;

public class CreateIndexesOperation implements Operation<Void> {
    private final List<Index> indexes;
    private final MongoNamespace namespace;

    public CreateIndexesOperation(final List<Index> indexes, final MongoNamespace namespace) {
        this.indexes = indexes;
        this.namespace = namespace;
    }

    @Override
    public Void execute(final Session session) {
        if (getPrimaryConnectionProvider(session).getServerDescription().getVersion()
                                                                 .compareTo(new ServerVersion(2, 6)) >= 0) {
            try {
                executeCommandBasedProtocol(session);
            } catch (MongoCommandFailureException e) {
                if (e.getErrorCode() == 11000) {
                    throw new MongoDuplicateKeyException(e.getErrorCode(), e.getErrorMessage(), e.getCommandResult());
                } else {
                    throw e;
                }
            }
        } else {
            executeCollectionBasedProtocol(session);
        }
        return null;

    }

    @SuppressWarnings("unchecked")
    private void executeCollectionBasedProtocol(final Session session) {
        MongoNamespace systemIndexes = new MongoNamespace(namespace.getDatabaseName(), "system.indexes");
        for (Index index : indexes) {
            executeProtocol(new InsertProtocol<Document>(systemIndexes, true, WriteConcern.ACKNOWLEDGED,
                                                         asList(new InsertRequest<Document>(toDocument(index))),
                                                         new DocumentCodec()),
                            getPrimaryConnectionProvider(session));
        }
    }


    private void executeCommandBasedProtocol(final Session session) {
        Document command = new Document("createIndexes", namespace.getCollectionName());
        List<Document> list = new ArrayList<Document>();
        for (Index index : indexes) {
            list.add(toDocument(index));
        }
        command.append("indexes", list);


        executeProtocol(new CommandProtocol(namespace.getDatabaseName(), command,
                                            new DocumentCodec(),
                                            new DocumentCodec()),
                        getPrimaryConnectionProvider(session));
    }

    private Document toDocument(final Index index) {
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
