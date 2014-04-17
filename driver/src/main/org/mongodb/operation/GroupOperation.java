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
import org.mongodb.Function;
import org.mongodb.MongoAsyncCursor;
import org.mongodb.MongoCursor;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.session.Session;

import java.util.List;

import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;
import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocolAsync;
import static org.mongodb.operation.OperationHelper.transformResult;

/**
 * Groups documents in a collection by the specified key and performs simple aggregation functions, such as computing counts and sums. The
 * command is analogous to a SELECT <...> GROUP BY statement in SQL.
 *
 * @mongodb.driver.manual reference/command/group Group Command
 * @since 3.0
 */
public class GroupOperation implements AsyncOperation<MongoAsyncCursor<Document>>, Operation<MongoCursor<Document>> {
    private final MongoNamespace namespace;
    private final Group group;
    private final ReadPreference readPreference;

    /**
     * Create an operation that will perform a Group on a given collection.
     * @param namespace      the database and collection to run the operation against
     * @param group          contains all the arguments for this group command
     * @param readPreference the ReadPreference for the group command. If null, primary will be used.
     */
    public GroupOperation(final MongoNamespace namespace, final Group group, final ReadPreference readPreference) {
        this.namespace = namespace;
        this.group = group;
        this.readPreference = readPreference;
    }

    /**
     * Will return a cursor of Documents containing the results of the group operation.
     *
     * @return a MongoCursor of T, the results of the group operation in a form to be iterated over
     * @param session the session
     */
    @Override
    @SuppressWarnings("unchecked")
    public MongoCursor<Document> execute(final Session session) {
        CommandResult result = executeWrappedCommandProtocol(namespace, getCommand(), readPreference, session);
        return transformResult(result, transform());
    }

    /**
     * Will return a cursor of Documents containing the results of the group operation.
     *
     * @return a Future MongoCursor of T, the results of the group operation in a form to be iterated over
     * @param session the session
     */
    @Override
    @SuppressWarnings("unchecked")
    public MongoFuture<MongoAsyncCursor<Document>> executeAsync(final Session session) {
        MongoFuture<CommandResult> result = executeWrappedCommandProtocolAsync(namespace, getCommand(), readPreference, session);
        return transformResult(result, transformAsync());
    }

    private Function<CommandResult, MongoCursor<Document>> transform() {
        return new Function<CommandResult, MongoCursor<Document>>() {
            @SuppressWarnings("unchecked")
            @Override
            public MongoCursor<Document> apply(final CommandResult result) {
                return new InlineMongoCursor<Document>(result.getAddress(), (List<Document>) result.getResponse().get("retval"));
            }
        };
    }

    private Function<CommandResult, MongoAsyncCursor<Document>> transformAsync() {
        return new Function<CommandResult, MongoAsyncCursor<Document>>() {
            @SuppressWarnings("unchecked")
            @Override
            public MongoAsyncCursor<Document> apply(final CommandResult result) {
                return new InlineMongoAsyncCursor<Document>((List<Document>) result.getResponse().get("retval"));
            }
        };
    }


    private Document getCommand() {

        Document document = new Document("ns", namespace.getCollectionName());

        if (group.getKey() != null) {
            document.put("key", group.getKey());
        } else {
            document.put("keyf", group.getKeyFunction());
        }

        document.put("initial", group.getInitial());
        document.put("$reduce", group.getReduceFunction());

        if (group.getFinalizeFunction() != null) {
            document.put("finalize", group.getFinalizeFunction());
        }

        if (group.getFilter() != null) {
            document.put("cond", group.getFilter());
        }

        return new Document("group", document);
    }
}
