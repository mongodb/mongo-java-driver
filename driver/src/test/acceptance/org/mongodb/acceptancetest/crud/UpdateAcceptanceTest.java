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

import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UpdateAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldSetANewFieldOnAnExistingDocument() {
        // given
        Document originalDocument = new Document("_id", 1);
        collection.insert(originalDocument);

        // when
        collection.find(new Document("_id", 1)).update(new Document("$set", new Document("x", 2)));

        // then
        assertThat(collection.find().count(), is(1L));
        Document expectedDocument = new Document("_id", 1).append("x", 2);
        assertThat(collection.find().getOne(), is(expectedDocument));
    }

    @Test
    public void shouldIncrementAnIntValue() {
        // given
        Document originalDocument = new Document("_id", 1).append("x", 2);
        collection.insert(originalDocument);

        // when
        collection.find(new Document("_id", 1)).update(new Document("$inc", new Document("x", 1)));

        // then
        assertThat(collection.find().count(), is(1L));
        Document expectedDocument = new Document("_id", 1).append("x", 3);
        assertThat(collection.find().getOne(), is(expectedDocument));
    }

    @Test
    public void shouldInsertTheDocumentIfUpdatingWithUpsertAndDocumentNotFoundInCollection() {
        // given
        Document originalDocument = new Document("_id", 1).append("x", 2);
        collection.insert(originalDocument);

        // when
        collection.find(new Document("_id", 2)).upsert().update(new Document("$set", new Document("x", 5)));

        // then
        assertThat(collection.find().count(), is(2L));
        Document expectedDocument = new Document("_id", 2).append("x", 5);
        assertThat(collection.find(new Document("_id", 2)).getOne(), is(expectedDocument));
    }

    @Test
    public void shouldInsertTheDocumentIfReplacingWithUpsertAndDocumentNotFoundInCollection() {
        // given
        assertThat(collection.find().count(), is(0L));

        // when
        Document replacement = new Document("_id", 3).append("x", 2);
        collection.find().upsert().replace(replacement);

        // then
        assertThat(collection.find().count(), is(1L));
        assertThat(collection.find(new Document("_id", 3)).getOne(), is(replacement));
    }

    @Test
    public void shouldReplaceTheDocumentIfReplacingWithUpsertAndDocumentIsFoundInCollection() {
        // given
        Document originalDocument = new Document("_id", 3).append("x", 2);
        collection.find().upsert().replace(originalDocument);
        assertThat(collection.find().count(), is(1L));

        // when
        Document replacement = originalDocument.append("y", 5);
        collection.find().upsert().replace(replacement);

        // then
        assertThat(collection.find().count(), is(1L));
        assertThat(collection.find(new Document("_id", 3)).getOne(), is(replacement));
    }
}
