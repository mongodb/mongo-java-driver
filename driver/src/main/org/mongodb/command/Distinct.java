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

package org.mongodb.command;

import org.mongodb.Document;
import org.mongodb.operation.MongoCommand;
import org.mongodb.operation.MongoFind;

public class Distinct extends MongoCommand {
    public Distinct(final String collectionName, final String fieldName, final MongoFind query) {
        super(toDocument(collectionName, fieldName, query));
    }

    private static Document toDocument(final String collectionName, final String fieldName,
                                              final MongoFind query) {
        final Document cmd = new Document("distinct", collectionName);
        cmd.put("key", fieldName);
        if (query.getFilter() != null) {
            cmd.put("query", query.getFilter());
        }
        return cmd;
    }
}