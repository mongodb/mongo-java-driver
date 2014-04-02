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

package org.mongodb;

import category.Async;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Category(Async.class)
public class MongoAsyncWriteTest extends DatabaseTestCase {
    @Test
    public void testReplaceOrInsertFuture() throws ExecutionException, InterruptedException {
        Document document = new Document();
        Future<WriteResult> resultFuture = collection.find().upsert().asyncReplace(document);
        WriteResult result = resultFuture.get();
        assertNotNull(result);
        assertEquals(collection.find().getOne(), document);
    }

    @Test
    public void testManyInsertsAsync() throws ExecutionException, InterruptedException {
        int numberOfInserts = 500;
        ArrayList<MongoFuture<WriteResult>> futures = new ArrayList<MongoFuture<WriteResult>>();

        for (int i = 0; i < numberOfInserts; i++) {
            Document doc = new Document();
            doc.put("_id", i);
            doc.put("field", "Some value");
            futures.add(collection.asyncInsert(doc));
        }

        for (MongoFuture<WriteResult> future : futures) {
            future.get();
        }

        assertEquals(collection.find().asyncCount().get().intValue(), numberOfInserts);
    }

}
