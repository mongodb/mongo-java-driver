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

package org.mongodb;

import org.bson.types.Document;
import org.junit.Test;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoSave;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MongoSaveTest extends MongoClientTestBase{
    @Test
    public void shouldInsertIfAbsent() {
        Document document = new Document();
        collection.save(new MongoSave<Document>(document));
        assertThat("Did not insert the document", collection.count(), is(1L));
    }

    @Test
    public void shouldReplaceIfPresent() {
        Document document = new Document();
        collection.save(new MongoSave<Document>(document));

        document.put("x", 1);
        collection.save(new MongoSave<Document>(document));
        assertThat("Did not replace the document", collection.findOne(new MongoFind()), is(document));
    }

    @Test
    public void shouldUpsertIfAbsent() {
        Document document = new Document("_id", 1);
        collection.save(new MongoSave<Document>(document));
        assertThat("Did not upsert the document", collection.findOne(new MongoFind()), is(document));
    }
}
