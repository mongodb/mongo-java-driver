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

package org.mongodb.acceptancetest.crud.pojo;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.Fixture;
import org.mongodb.MongoCollection;
import org.mongodb.codecs.Codecs;
import org.mongodb.codecs.PojoCodec;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

@Ignore("Rather oddly, this passes in IntelliJ but not on the command line.")
//TODO:might be our old friend jacoco
public class WorkingWithPojosAcceptanceTest extends DatabaseTestCase {
    private static final String COLLECTION_NAME = "PojoCollectionName";
    private MongoCollection<Person> pojoCollection;

    @Before
    public void setUp() {
        pojoCollection = Fixture.getMongoClient()
                                .getDatabase(getDatabaseName())
                                .getCollection(COLLECTION_NAME, new PojoCodec<Person>(Codecs.createDefault()));
        pojoCollection.tools().drop();
    }

    @Test
    public void shouldInsertASimplePojoIntoMongoDB() {
        final Person person = new Person("Eric", "Smith");
        pojoCollection.insert(person);

        assertInsertedIntoDatabase(person);
    }

    private void assertInsertedIntoDatabase(final Person person) {
        final MongoCollection<Document> personCollection = database.getCollection(COLLECTION_NAME);

        final List<Document> results = new ArrayList<Document>();
        personCollection.filter(new Document("firstName", person.getFirstName())
                                .append("lastName", person.getLastName())).into(results);

        assertThat(results.size(), is(1));
    }

}
