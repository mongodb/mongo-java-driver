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

package com.mongodb.acceptancetest.core;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.client.DatabaseTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class MongoPipelineAcceptanceTest extends DatabaseTestCase {
    private final List<Document> documents = new ArrayList<Document>();

    @Before
    public void setUp() {
        super.setUp();
        documents.add(new Document("_id", "01778").append("city", "WAYLAND").append("state", "MA").append("population", 13100)
                                                  .append("loc", asList(42.3635, 71.3619)).append("tags", asList("driver")));
        documents.add(new Document("_id", "10012").append("city", "NEW YORK CITY")
                                                  .append("state", "NY")
                                                  .append("population", 8245000)
                                                  .append("loc", asList(40.7260, 71.3619))
                                                  .append("tags", asList("driver", "SA", "CE", "kernel")));
        documents.add(new Document("_id", "94301").append("city", "PALO ALTO")
                                                  .append("state", "CA")
                                                  .append("population", 65412)
                                                  .append("loc", asList(37.4419, 122.1419))
                                                  .append("tags", asList("driver", "SA", "CE")));

        List<Document> shuffledDocuments = new ArrayList<Document>(documents);

        Collections.shuffle(shuffledDocuments);
        for (final Document doc : shuffledDocuments) {
            collection.insert(doc);
        }
    }

    @Test
    public void shouldIterateOverAllDocumentsInCollection() {
        List<Document> iteratedDocuments = new ArrayList<Document>();
        for (final Document cur : collection.pipe()) {
            iteratedDocuments.add(cur);
        }
        assertEquals(3, iteratedDocuments.size());
    }

    @Test
    public void shouldForEachOverAllDocumentsInCollection() {
        final List<Document> iteratedDocuments = new ArrayList<Document>();
        collection.pipe().forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                iteratedDocuments.add(document);
            }
        });
        assertEquals(3, iteratedDocuments.size());
    }


    @Test
    public void shouldAddAllDocumentsIntoList() {
        List<Document> iteratedDocuments = collection.pipe().into(new ArrayList<Document>());
        assertEquals(3, iteratedDocuments.size());
    }

    @Test
    public void shouldMapAllDocumentsIntoList() {
        List<String> iteratedDocuments = collection.pipe().map(new Function<Document, String>() {
            @Override
            public String apply(final Document document) {
                return document.getString("_id");
            }
        }).into(new ArrayList<String>());
        Collections.sort(iteratedDocuments);
        assertEquals(asList("01778", "10012", "94301"), iteratedDocuments);
    }

    @Test
    public void shouldSortDocuments() {
        List<Document> sorted = collection.pipe().sort(new Document("_id", 1)).into(new ArrayList<Document>());
        assertEquals(documents, sorted);
    }

    @Test
    public void shouldSkipDocuments() {
        List<Document> skipped = collection.pipe().sort(new Document("_id", 1)).skip(1).into(new ArrayList<Document>());
        assertEquals(documents.subList(1, 3), skipped);
    }

    @Test
    public void shouldLimitDocuments() {
        List<Document> limited = collection.pipe().sort(new Document("_id", 1)).limit(2).into(new ArrayList<Document>());
        assertEquals(documents.subList(0, 2), limited);
    }

    @Test
    public void shouldFindDocuments() {
        List<Document> matched = collection.pipe().find(new Document("_id", "10012")).into(new ArrayList<Document>());
        assertEquals(documents.subList(1, 2), matched);
    }

    @Test
    public void shouldProjectDocuments() {
        List<Document> sorted = collection.pipe().sort(new Document("_id", 1)).project(new Document("_id", 0).append("zip", "$_id"))
                                          .into(new ArrayList<Document>());
        assertEquals(asList(new Document("zip", "01778"), new Document("zip", "10012"), new Document("zip", "94301")), sorted);
    }

    @Test
    public void shouldUnwindDocuments() {
        List<Document> unwound = collection.pipe().sort(new Document("_id", 1))
                                           .project(new Document("_id", 0).append("tags", 1))
                                           .unwind("$tags")
                                           .into(new ArrayList<Document>());
        assertEquals(asList(
                               new Document("tags", "driver"),
                               new Document("tags", "driver"),
                               new Document("tags", "SA"),
                               new Document("tags", "CE"),
                               new Document("tags", "kernel"),
                               new Document("tags", "driver"),
                               new Document("tags", "SA"),
                               new Document("tags", "CE")),
                     unwound);
    }

    @Test
    public void shouldGroupDocuments() {
        List<Document> grouped = collection.pipe().sort(new Document("_id", 1))
                                           .project(new Document("_id", 0).append("tags", 1))
                                           .unwind("$tags")
                                           .group(new Document("_id", "$tags"))
                                           .sort(new Document("_id", 1))
                                           .into(new ArrayList<Document>());
        assertEquals(asList(new Document("_id", "CE"),
                            new Document("_id", "SA"),
                            new Document("_id", "driver"),
                            new Document("_id", "kernel")),
                     grouped);
    }
}
