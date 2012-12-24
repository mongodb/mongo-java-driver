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

public class MongoStreamTest extends MongoClientTestBase {

    @Test
    public void testFind() {
        for (int i = 0; i < 10; i++) {
            collection.insert(new Document("_id", i));
        }

        for (Document cur : collection) {
            System.out.println(cur);
        }

        for (Document cur : collection.filter(new QueryFilterDocument("_id", 1))) {
            System.out.println(cur);
        }

        for (Document cur : collection.filter(new QueryFilterDocument("_id", 1)).sort(
                new SortCriteriaDocument("_id", 1))) {
            System.out.println(cur);
        }

        for (Document cur : collection.skip(3).limit(2).sort(new SortCriteriaDocument("_id", -1))) {
            System.out.println(cur);
        }

        long count = collection.count();
        System.out.println(count);

        count = collection.filter(new QueryFilterDocument("_id", new Document("$gt", 2))).count();
        System.out.println(count);

        Document doc = collection.findOne();
        System.out.println(doc);

        doc = collection.filter(new QueryFilterDocument("_id", 1)).findOne();
        System.out.println(doc);

        collection.forEach(e -> System.out.println(e));

        collection.forEach(System.out::println);

        collection.forEach(new Block<Document>() {
            public void run(final Document document) {
                System.out.println(document);
            }
        });
    }

    @Test
    public void testUpdate() {
        collection.insert(new Document("_id", 1));

        collection.update(new UpdateOperationsDocument("$set", new Document("x", 1)));

        collection.filter(
                new QueryFilterDocument("_id", 1)).update(new UpdateOperationsDocument("$set", new Document("x", 1)));

        collection.filter(
                new QueryFilterDocument("_id", 2)).upsert().update(
                new UpdateOperationsDocument("$set", new Document("x", 1)));

        Document doc = collection.filter(new QueryFilterDocument("_id", 1)).
                findAndUpdate(new UpdateOperationsDocument("$set", new Document("x", 1)));
        System.out.println(doc);
    }
}
