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

package org.mongodb.acceptancetest.core;

import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoClient;

import java.util.Set;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mongodb.Fixture.getMongoClient;

public class DatabaseAdminAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldListAllTheDatabasesAvailable() {
        //given
        final MongoClient mongoClient = getMongoClient();
        mongoClient.getDatabase("FirstNewDatabase").getCollection("coll").insert(new Document("aDoc", "to force database creation"));
        mongoClient.getDatabase("SecondNewDatabase").getCollection("coll").insert(new Document("aDoc", "to force database creation"));
        mongoClient.getDatabase("DatabaseThatDoesNotExistYet");

        //when
        final Set<String> databaseNames = mongoClient.tools().getDatabaseNames();

        //then
        assertThat(databaseNames, hasItems("FirstNewDatabase", "SecondNewDatabase", "admin", "local", getDatabaseName()));
        assertThat(databaseNames, not(hasItem("DatabaseThatDoesNotExistYet")));

        //tear down
        mongoClient.getDatabase("FirstNewDatabase").tools().drop();
        mongoClient.getDatabase("SecondNewDatabase").tools().drop();
    }
}
