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

public class Group extends Command {

    public Group(final org.mongodb.operation.Group group, final String collectionName) {
        super(asDocument(group, collectionName));
    }

    private static Document asDocument(final org.mongodb.operation.Group group, final String collectionName) {

        final Document document = new Document("ns", collectionName);

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
