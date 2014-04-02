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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.session.Session;

import java.util.List;

import static org.mongodb.operation.OperationHelper.executeWrappedCommandProtocol;

/**
 * Finds the distinct values for a specified field across a single collection. This returns an array of the distinct values.
 * <p/>
 * When possible, the distinct command uses an index to find documents and return values.
 * <p/>
 *
 * @mongodb.driver.manual reference/command/distinct Distinct Command
 * @since 3.0
 */
public class DistinctOperation implements Operation<MongoCursor<String>> {
    private final MongoNamespace namespace;
    private final String fieldName;
    private final Find find;

    /**
     * This operation will return the results of the query with no duplicate entries for the selected field.
     *
     * @param namespace the database and collection to run the query against
     * @param fieldName the field that needs to be distinct
     * @param find      the query criteria
     */
    public DistinctOperation(final MongoNamespace namespace, final String fieldName, final Find find) {
        this.namespace = namespace;
        this.fieldName = fieldName;
        this.find = find;
    }

    @Override
    @SuppressWarnings("unchecked")
    public MongoCursor<String> execute(final Session session) {
        CommandResult commandResult = executeWrappedCommandProtocol(namespace, asCommandDocument(), new DocumentCodec(),
                                                                    new DocumentCodec(), find.getReadPreference(), session);

        return new InlineMongoCursor<String>(commandResult.getAddress(), (List<String>) commandResult.getResponse().get("values"));
    }

    private Document asCommandDocument() {
        Document cmd = new Document("distinct", namespace.getCollectionName());
        cmd.put("key", fieldName);
        if (find.getFilter() != null) {
            cmd.put("query", find.getFilter());
        }
        return cmd;
    }
}
