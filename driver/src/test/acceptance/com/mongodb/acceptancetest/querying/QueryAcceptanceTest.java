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

package com.mongodb.acceptancetest.querying;

import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.BsonObjectId;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.type;
import static com.mongodb.client.model.Sorts.descending;
import static java.util.Arrays.asList;
import static org.bson.BsonType.INT32;
import static org.bson.BsonType.INT64;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class QueryAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldBeAbleToQueryWithDocumentSpecification() {
        collection.insertOne(new Document("name", "Bob"));

        Document query = new Document("name", "Bob");
        MongoCursor<Document> results = collection.find().filter(query).iterator();

        assertThat(results.next().get("name").toString(), is("Bob"));
    }

    @Test
    public void shouldBeAbleToQueryWithDocument() {
        collection.insertOne(new Document("name", "Bob"));

        Document query = new Document("name", "Bob");
        MongoCursor<Document> results = collection.find(query).iterator();

        assertThat(results.next().get("name").toString(), is("Bob"));
    }

    @Test
    public void shouldBeAbleToQueryTypedCollectionWithDocument() {
        CodecRegistry codecRegistry = fromProviders(asList(new ValueCodecProvider(), new DocumentCodecProvider(),
                new BsonValueCodecProvider(), new PersonCodecProvider()));
        MongoCollection<Person> collection = database
                .getCollection(getCollectionName(), Person.class)
                .withCodecRegistry(codecRegistry);
        collection.insertOne(new Person("Bob"));

        MongoCursor<Person> results = collection.find(new Document("name", "Bob")).iterator();

        assertThat(results.next().name, is("Bob"));
    }

    @Test
    public void shouldBeAbleToFilterByType() {
        collection.insertOne(new Document("product", "Book").append("numTimesOrdered", "some"));
        collection.insertOne(new Document("product", "CD").append("numTimesOrdered", "6"));
        collection.insertOne(new Document("product", "DVD").append("numTimesOrdered", 9));
        collection.insertOne(new Document("product", "SomethingElse").append("numTimesOrdered", 10));

        List<Document> results = new ArrayList<Document>();
        collection.find(new Document("numTimesOrdered", new Document("$type", 16)))
                  .sort(new Document("numTimesOrdered", -1)).into(results);

        assertThat(results.size(), is(2));
        assertThat(results.get(0).get("product").toString(), is("SomethingElse"));
        assertThat(results.get(1).get("product").toString(), is("DVD"));
    }

    @Test
    public void shouldUseFriendlyQueryType() {
        collection.insertOne(new Document("product", "Book").append("numTimesOrdered", "some"));
        collection.insertOne(new Document("product", "CD").append("numTimesOrdered", "6"));
        collection.insertOne(new Document("product", "DVD").append("numTimesOrdered", 9));
        collection.insertOne(new Document("product", "SomethingElse").append("numTimesOrdered", 10));
        collection.insertOne(new Document("product", "VeryPopular").append("numTimesOrdered", 7843273657286478L));

        List<Document> results = new ArrayList<Document>();
        //TODO make BSON type serializable
        Document filter = new Document("$or", asList(new Document("numTimesOrdered", new Document("$type", INT32.getValue())),
                                                     new Document("numTimesOrdered", new Document("$type", INT64.getValue()))));
        collection.find(filter).sort(new Document("numTimesOrdered", -1)).into(results);

        assertThat(results.size(), is(3));
        assertThat(results.get(0).get("product").toString(), is("VeryPopular"));
        assertThat(results.get(1).get("product").toString(), is("SomethingElse"));
        assertThat(results.get(2).get("product").toString(), is("DVD"));
    }

    @Test
    public void shouldBeAbleToSortAscending() {
        collection.insertOne(new Document("product", "Book"));
        collection.insertOne(new Document("product", "DVD"));
        collection.insertOne(new Document("product", "CD"));

        List<Document> results = new ArrayList<Document>();
        collection.find().sort(new Document("product", 1)).into(results);

        assertThat(results.size(), is(3));
        assertThat(results.get(0).get("product").toString(), is("Book"));
        assertThat(results.get(1).get("product").toString(), is("CD"));
        assertThat(results.get(2).get("product").toString(), is("DVD"));
    }

    @Test
    public void shouldBeAbleToUseQueryBuilderForFilter() {
        collection.insertOne(new Document("product", "Book").append("numTimesOrdered", "some"));
        collection.insertOne(new Document("product", "CD").append("numTimesOrdered", "6"));
        collection.insertOne(new Document("product", "DVD").append("numTimesOrdered", 9));
        collection.insertOne(new Document("product", "SomethingElse").append("numTimesOrdered", 10));
        collection.insertOne(new Document("product", "VeryPopular").append("numTimesOrdered", 7843273657286478L));

        List<Document> results = new ArrayList<Document>();

        collection.find(or(type("numTimesOrdered", INT32), type("numTimesOrdered", INT64)))
                  .sort(descending("numTimesOrdered")).into(results);

        assertThat(results.size(), is(3));
        assertThat(results.get(0).get("product").toString(), is("VeryPopular"));
        assertThat(results.get(1).get("product").toString(), is("SomethingElse"));
        assertThat(results.get(2).get("product").toString(), is("DVD"));
    }


    @Test
    @Ignore("JSON stuff not implemented")
    public void shouldBeAbleToQueryWithJSON() {
    }

    private class PersonCodecProvider implements CodecProvider {

        @Override
        @SuppressWarnings("unchecked")
        public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
            if (clazz.equals(Person.class)) {
                return (Codec<T>) new PersonCodec();
            }

            return null;
        }
    }

    private class PersonCodec implements CollectibleCodec<Person> {
        @Override
        public boolean documentHasId(final Person document) {
            return true;
        }

        @Override
        public BsonObjectId getDocumentId(final Person document) {
            return new BsonObjectId(document.id);
        }

        @Override
        public Person generateIdIfAbsentFromDocument(final Person person) {
            return person;
        }

        @Override
        public void encode(final BsonWriter writer, final Person value, final EncoderContext encoderContext) {
            writer.writeStartDocument();
            writer.writeObjectId("_id", value.id);
            writer.writeString("name", value.name);
            writer.writeEndDocument();
        }

        @Override
        public Person decode(final BsonReader reader, final DecoderContext decoderContext) {
            reader.readStartDocument();
            ObjectId id = reader.readObjectId("_id");
            String name = reader.readString("name");
            reader.readEndDocument();
            return new Person(id, name);
        }

        @Override
        public Class<Person> getEncoderClass() {
            return Person.class;
        }
    }

    private class Person {
        private ObjectId id = new ObjectId();
        private final String name;

        Person(final String name) {
            this.name = name;
        }

        Person(final ObjectId id, final String name) {
            this.id = id;
            this.name = name;
        }
    }

}
