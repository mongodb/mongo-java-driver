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
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class WorkingWithPojosAcceptanceTest extends DatabaseTestCase {
    private static final String COLLECTION_NAME = "PojoCollectionName";
    private MongoCollection<Person> pojoCollection;

    @Before
    public void setUp() {
        super.setUp();
        pojoCollection = Fixture.getMongoClient()
                                .getDatabase(getDatabaseName())
                                .getCollection(COLLECTION_NAME,
                                               new PojoCodec<Person>(Codecs.createDefault(), Person.class));
        pojoCollection.tools().drop();
    }

    @Test
    public void shouldInsertASimplePojoIntoMongoDB() {
        Person person = new Person("Eric", "Smith");
        pojoCollection.insert(person);

        assertInsertedIntoDatabase(person);
    }

    @Test
    public void shouldInsertASimplePojoIntoDatabaseAndRetrieveAsPojo() {
        Person person = new Person("Ian", "Brown");
        pojoCollection.insert(person);

        Person result = pojoCollection.find(new Document("firstName", person.getFirstName())
                                                .append("lastName", person.getLastName())).getOne();
        assertThat(result, is(person));
    }

    @Test
    public void shouldCorrectlyInsertAndRetrievePojosContainingOtherPojos() {
        MongoCollection<Address> addresses = Fixture.getMongoClient()
                                                    .getDatabase(getDatabaseName())
                                                    .getCollection("addresses",
                                                                   new PojoCodec<Address>(Codecs.createDefault(), Address.class));
        addresses.tools().drop();

        Address address = new Address("Address Line 1", "Town", new Postcode("W12"));

        addresses.insert(address);

        Address result = addresses.find().getOne();
        assertThat(result, is(address));
    }

    @Test
    public void shouldIgnoreTransientFields() {
        Person person = new Person("Bob", "Smith");
        pojoCollection.insert(person);

        MongoCollection<Document> personCollection = database.getCollection(COLLECTION_NAME);

        Document personInCollection = personCollection.find(new Document("firstName", person.getFirstName())
                                                                .append("lastName", person.getLastName()))
                                                      .getOne();

        assertThat(personInCollection.get("ignoredValue"), is(nullValue()));
    }

    private void assertInsertedIntoDatabase(final Person person) {
        MongoCollection<Document> personCollection = database.getCollection(COLLECTION_NAME);

        List<Document> results = new ArrayList<Document>();
        personCollection.find(new Document("firstName", person.getFirstName())
                                  .append("lastName", person.getLastName())).into(results);

        assertThat(results.size(), is(1));
    }

}
