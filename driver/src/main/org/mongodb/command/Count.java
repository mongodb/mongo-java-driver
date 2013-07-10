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
import org.mongodb.MongoNamespace;
import org.mongodb.operation.Find;
import org.mongodb.operation.Query;

public final class Count extends Command {
    private final MongoNamespace namespace;

    public Count(final Find find, final MongoNamespace namespace) {
        super(asDocument(find, namespace.getCollectionName()));
        readPreference(find.getReadPreference());
        this.namespace = namespace;
    }

    private static Document asDocument(final Find find, final String collectionName) {

        final Document document = new Document("count", collectionName);

        if (find.getFilter() != null) {
            document.put("query", find.getFilter());
        }
        if (find.getLimit() > 0) {
            document.put("limit", find.getLimit());
        }
        if (find.getSkip() > 0) {
            document.put("skip", find.getSkip());
        }

        return document;
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    @Override
    public Query skip(final int skip) {
        throw new UnsupportedOperationException("Set skip on the instance of Find passed to the constructor");
    }

    @Override
    public Query limit(final int limit) {
        throw new UnsupportedOperationException("Set skip on the instance of Find passed to the constructor");
    }
}
