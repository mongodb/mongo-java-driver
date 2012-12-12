/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package org.mongodb.command;

import org.mongodb.MongoClient;
import org.mongodb.MongoCommandDocument;
import org.mongodb.MongoDocument;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.result.CommandResult;

// TODO: deal with !ok responses where the reason is because the collection does not exist.
public class CountCommand extends AbstractCommand {
    private final MongoNamespace namespace;
    private final MongoFind find;

    public CountCommand(final MongoClient mongoClient, final MongoNamespace namespace) {
        this(mongoClient, namespace, new MongoFind());
    }

    public CountCommand(final MongoClient client, final MongoNamespace namespace, final MongoFind find) {
        super(client, namespace.getDatabaseName());
        this.namespace = namespace;
        this.find = find;
    }

    @Override
    public CountCommandResult execute() {
        return new CountCommandResult(getMongoClient().getOperations().executeCommand(getDatabase(),
                new MongoCommandOperation(asMongoCommand()), createResultSerializer()));
    }

    @Override
    public MongoCommand asMongoCommand() {
        final MongoCommandDocument document = new MongoCommandDocument("count", namespace.getCollectionName());

        if (find.getFilter() != null) {
            document.put("query", find.getFilter().toMongoDocument());
        }

        if (find.getLimit() > 0) {
            document.put("limit", find.getLimit());
        }

        if (find.getNumToSkip() > 0) {
            document.put("skip", find.getNumToSkip());
        }

        return document;
    }

    public static class CountCommandResult extends CommandResult {

        public CountCommandResult(final MongoDocument mongoDocument) {
            super(mongoDocument);
        }

        public long getCount() {
            return ((Double) getMongoDocument().get("n")).longValue();
        }
    }
}
