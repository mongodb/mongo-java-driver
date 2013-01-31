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

package org.mongodb.acceptancetest.crud;

import org.bson.types.Document;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.MongoCursor;
import org.mongodb.QueryFilterDocument;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class InsertMongoDocumentAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldInsertSimpleUntypedDocument() {
        final Document simpleDocument = new Document("name", "Billy");
        collection.insert(simpleDocument);

        assertThat(collection.count(), is(1L));

        final QueryFilterDocument queryFilter = new QueryFilterDocument("name", "Billy");
        final MongoCursor<Document> insertTestDocumentMongoCursor = collection.filter(queryFilter).all();

        assertThat((String) insertTestDocumentMongoCursor.next().get("name"), is("Billy"));
    }
}
