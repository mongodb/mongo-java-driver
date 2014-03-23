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

import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.MongoCursor;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.session.Session;

import java.util.List;

import static org.mongodb.operation.OperationHelper.executeProtocol;

/**
 * Groups documents in a collection by the specified key and performs simple aggregation functions, such as computing counts and sums. The
 * command is analogous to a SELECT <...> GROUP BY statement in SQL.
 *
 * @mongodb.driver.manual reference/command/group Group Command
 * @since 3.0
 */
public class GroupOperation implements Operation<MongoCursor<Document>> {
    private final MongoNamespace namespace;
    private final Document commandDocument;
    private final ReadPreference readPreference;

    /**
     * Create an operation that will perform a Group on a given collection.
     * @param namespace      the database and collection to run the operation against
     * @param group          contains all the arguments for this group command
     * @param readPreference the ReadPreference for the group command. If null, primary will be used.
     */
    public GroupOperation(final MongoNamespace namespace, final Group group, final ReadPreference readPreference) {
        this.namespace = namespace;
        this.commandDocument = createCommandDocument(namespace, group);
        this.readPreference = readPreference;
    }

    /**
     * Will return a cursor of Documents containing the results of the group operation.
     *
     * @return a MongoCursor of T, the results of the group operation in a form to be iterated over
     * @param session
     */
    @Override
    @SuppressWarnings("unchecked")
    public MongoCursor<Document> execute(final Session session) {
        CommandResult commandResult = executeProtocol(new CommandProtocol(namespace.getDatabaseName(), commandDocument,
                                                                          new DocumentCodec(), new DocumentCodec()),
                                                      readPreference, session);

        return new InlineMongoCursor<Document>(commandResult, (List<Document>) commandResult.getResponse().get("retval"));
    }

    private Document createCommandDocument(final MongoNamespace namespace, final Group commandDocument) {

        Document document = new Document("ns", namespace.getCollectionName());

        if (commandDocument.getKey() != null) {
            document.put("key", commandDocument.getKey());
        } else {
            document.put("keyf", commandDocument.getKeyFunction());
        }

        document.put("initial", commandDocument.getInitial());
        document.put("$reduce", commandDocument.getReduceFunction());

        if (commandDocument.getFinalizeFunction() != null) {
            document.put("finalize", commandDocument.getFinalizeFunction());
        }

        if (commandDocument.getFilter() != null) {
            document.put("cond", commandDocument.getFilter());
        }

        return new Document("group", document);
    }
}
