/*
 * Copyright (c) 2015 MongoDB, Inc.
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

import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Documents the basic functionality available for the MongoClient via the Java driver.
 */
public class ClientAcceptanceTest extends DatabaseTestCase {

    @Test
    public void shouldListDatabaseNamesFromDatabase() {
        database.createCollection(getCollectionName());
        List<String> names = client.listDatabaseNames().into(new ArrayList<String>());

        assertThat(names.contains(getDatabaseName()), is(true));
    }

    @Test
    public void shouldBeAbleToListAllTheDatabaseNamesAvailable() {
        MongoDatabase firstDatabase = client.getDatabase("FirstNewDatabase");
        MongoDatabase secondDatabase = client.getDatabase("SecondNewDatabase");
        MongoDatabase otherDatabase = client.getDatabase("DatabaseThatDoesNotExistYet");

        try {
            // given
            firstDatabase.getCollection("coll").insertOne(new Document("aDoc", "to force database creation"));
            secondDatabase.getCollection("coll").insertOne(new Document("aDoc", "to force database creation"));

            //when
            List<String> databaseNames = client.listDatabaseNames().into(new ArrayList<String>());

            //then
            assertThat(databaseNames, hasItems(firstDatabase.getName(), secondDatabase.getName()));
            assertThat(databaseNames, not(hasItem(otherDatabase.getName())));
        } finally {
            //tear down
            firstDatabase.drop();
            secondDatabase.drop();
        }
    }

    @Test
    public void shouldListDatabase() {
        List<Document> databases = client.listDatabases().into(new ArrayList<Document>());

        database.createCollection(getCollectionName());
        databases = client.listDatabases().into(new ArrayList<Document>());
        assertThat(databases, new DatabaseNameMatcher(getDatabaseName()));
    }

    private static final class DatabaseNameMatcher extends BaseMatcher<List<Document>> {

        private final String name;

        DatabaseNameMatcher(final String name) {
            this.name = name;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean matches(final Object item) {
            List<Document> databases = (List<Document>) item;
            for (Document cur : databases) {
                if (cur.get("name").equals(name)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("Document containing a name of " + name);
        }
    }
}
