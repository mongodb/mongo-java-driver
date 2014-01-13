/*
 * Copyright (c) 2008 MongoDB, Inc.
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

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mongodb.QueryBuilder.query;

public class MongoViewTest extends DatabaseTestCase {

    // CHECKSTYLE:OFF
    @Test
    @Ignore
    public void testFind() {
        //TODO: this test really really needs re-writing. Or taking out the back and shooting.
        for (int i = 0; i < 10; i++) {
            collection.insert(new Document("_id", i));
        }

        for (final Document cur : collection.find()) {
            System.out.println(cur);
        }

        MongoCursor<Document> cursor = collection.find().get();
        try {
            while (cursor.hasNext()) {
                cursor.next();
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        for (final Document cur : collection.find(new Document("_id", 1))) {
            System.out.println(cur);
        }

        for (final Document cur : collection.find(new Document("_id", 1))
                                            .sort(new Document("_id", 1))) {
            System.out.println(cur);
        }

        System.out.println();

        for (final Document cur : collection.find(query("_id").greaterThan(4)).sort(new Document("_id", 1))) {
            System.out.println(cur);
        }

        System.out.println();

        for (final Document cur : collection.find().skip(3).limit(2).sort(new Document("_id", -1))) {
            System.out.println(cur);
        }

        long count = collection.find().count();
        System.out.println(count);

        count = collection.find(new Document("_id", new Document("$gt", 2))).count();
        System.out.println(count);

        Document doc = collection.find().getOne();
        System.out.println(doc);

        doc = collection.find(new Document("_id", 1)).getOne();
        System.out.println(doc);

        collection.find().forEach(new Block<Document>() {
            @Override
            public boolean run(final Document e) {
                System.out.println(e);
                return true;
            }
        });

        collection.find().forEach(new Block<Document>() {
            @Override
            public boolean run(final Document t) {
                System.out.println(t);
                return true;
            }
        });

        collection.find().forEach(new Block<Document>() {
            public boolean run(final Document document) {
                System.out.println(document);
                return true;
            }
        });

        for (final Integer id : collection.find().map(new Function<Document, Integer>() {
            @Override
            public Integer apply(final Document document) {
                return (Integer) document.get("_id");
            }
        })) {
            System.out.println(id);
        }

        List<String> list = collection.find().map(new Function<Document, Integer>() {
            @Override
            public Integer apply(final Document document) {
                return (Integer) document.get("_id");
            }
        }).map(new Function<Integer, String>() {
            @Override
            public String apply(final Integer integer) {
                return integer.toString();
            }
        }).into(new ArrayList<String>());

        System.out.println(list);

        collection.find().forEach(new Block<Document>() {
            @Override
            public boolean run(final Document t) {
                System.out.println(t);
                return true;
            }
        });

        List<Integer> idList = collection.find().map(new Function<Document, Integer>() {
            @Override
            public Integer apply(final Document document) {
                return (Integer) document.get("_id");
            }
        }).into(new ArrayList<Integer>());

        collection.find().sort(Sort.ascending("x")).skip(4).limit(5).getOne();
        cursor = collection.find().sort(new Document("price", 1)).skip(5).limit(1000)
                           .withQueryOptions(new QueryOptions().min(new Document("price", 0.99))
                                                               .max(new Document("price", 9.99))
                                                               .hint("price"))
                           .get();

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }
        cursor.close();
        System.out.println(idList);
    }
    // CHECKSTYLE:ON
}

