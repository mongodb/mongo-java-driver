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

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.mongodb.Document;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;
import org.mongodb.ConvertibleToDocument;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.acceptancetest.AcceptanceTestCase;
import org.mongodb.serialization.CollectibleSerializer;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QueryAcceptanceTest extends AcceptanceTestCase {
    @Test
    public void shouldBeAbleToQueryWithDocumentSpecification() {
        collection.insert(new Document("name", "Bob"));

        final Document query = new Document("name", "Bob");
        final MongoCursor<Document> results = collection.filter(query).all();

        assertThat(results.next().get("name").toString(), is("Bob"));
    }

    @Test
    public void shouldBeAbleToQueryWithDocument() {
        collection.insert(new Document("name", "Bob"));

        final Document query = new Document("name", "Bob");
        final MongoCursor<Document> results = collection.filter(query).all();

        assertThat(results.next().get("name").toString(), is("Bob"));
    }

    @Test
    public void shouldBeAbleToQueryTypedCollectionWithDocument() {
        final MongoCollection<Person> personCollection = database.getCollection(collectionName, new PersonSerializer());
        personCollection.insert(new Person("Bob"));

        final MongoCursor<Person> results = personCollection.filter(new Document("name", "Bob")).all();

        assertThat(results.next().name, is("Bob"));
    }

    @Test
    public void shouldBeAbleToQueryWithType() {
        final MongoCollection<Person> personCollection = database.getCollection(collectionName, new PersonSerializer());
        final Person bob = new Person("Bob");
        personCollection.insert(bob);

        final MongoCursor<Person> results = personCollection.filter(bob).all();

        assertThat(results.next().name, is("Bob"));
    }

    @Test
    @Ignore("JSON stuff not implemented")
    public void shouldBeAbleToQueryWithJSON() {
//        collection.insert(new Document("name", "Bob"));
//
//        final Document query = new Document("name", "Bob");
//        final MongoCursor<Document> results = collection.filter(query.toJSONString()).all();

//        assertThat(results.next().get("name").toString(), is("Bob"));
    }

    private class PersonSerializer implements CollectibleSerializer<Person> {
        @Override
        public Object getId(final Person person) {
            return person.id;
        }

        @Override
        public void serialize(final BSONWriter bsonWriter, final Person value) {
            bsonWriter.writeStartDocument();
            bsonWriter.writeObjectId("_id", value.id);
            bsonWriter.writeString("name", value.name);
            bsonWriter.writeEndDocument();
        }

        @Override
        public Person deserialize(final BSONReader reader) {
            reader.readStartDocument();
            final ObjectId id = reader.readObjectId("_id");
            final String name = reader.readString("name");
            reader.readEndDocument();
            return new Person(id, name);
        }

        @Override
        public Class<Person> getSerializationClass() {
            return Person.class;
        }
    }

    private class Person implements ConvertibleToDocument {
        private ObjectId id = new ObjectId();
        private String name;

        public Person(final String name) {
            this.name = name;
        }

        public Person(final ObjectId id, final String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public Document toDocument() {
            return new Document("name", name);
        }
    }
}
