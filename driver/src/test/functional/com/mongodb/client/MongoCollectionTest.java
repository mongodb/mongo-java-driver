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

package com.mongodb.client;

import com.mongodb.DBRef;
import com.mongodb.Function;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public class MongoCollectionTest extends DatabaseTestCase {

    @Test
    public void testFindAndUpdateWithGenerics() {
        CodecRegistry codecRegistry = fromProviders(asList(new ValueCodecProvider(), new DocumentCodecProvider(),
                new BsonValueCodecProvider(), new ConcreteCodecProvider()));
        MongoCollection<Concrete> collection = database
                .getCollection(getCollectionName())
                .withDocumentClass(Concrete.class)
                .withCodecRegistry(codecRegistry)
                .withReadPreference(ReadPreference.primary())
                .withWriteConcern(WriteConcern.ACKNOWLEDGED);

        Concrete doc = new Concrete(new ObjectId(), "str", 5, 10L, 4.0, 3290482390480L);
        collection.insertOne(doc);

        Concrete newDoc = collection.findOneAndUpdate(new Document("i", 5),
                                                      new Document("$set", new Document("i", 6)));

        assertNotNull(newDoc);
        assertEquals(doc, newDoc);
    }

    @Test
    public void shouldBeAbleToQueryTypedCollectionAndMapResultsIntoTypedLists() {
        // given
        CodecRegistry codecRegistry = fromProviders(asList(new ValueCodecProvider(), new DocumentCodecProvider(),
                new BsonValueCodecProvider(), new ConcreteCodecProvider()));
        MongoCollection<Concrete> collection = database
                .getCollection(getCollectionName())
                .withDocumentClass(Concrete.class)
                .withCodecRegistry(codecRegistry)
                .withReadPreference(ReadPreference.primary())
                .withWriteConcern(WriteConcern.ACKNOWLEDGED);

        Concrete firstItem = new Concrete("first", 1, 2L, 3.0, 5L);
        collection.insertOne(firstItem);

        Concrete secondItem = new Concrete("second", 7, 11L, 13.0, 17L);
        collection.insertOne(secondItem);

        // when
        List<String> listOfStringObjectIds = collection.find(new Document("i", 1))
                                                       .map(new Function<Concrete, ObjectId>() {
                                                           @Override
                                                           public ObjectId apply(final Concrete concrete) {
                                                               return concrete.getId();
                                                           }
                                                       })
                                                       .map(new Function<ObjectId, String>() {
                                                           @Override
                                                           public String apply(final ObjectId objectId) {
                                                               return objectId.toString();
                                                           }
                                                       }).into(new ArrayList<String>());

        // then
        assertThat(listOfStringObjectIds.size(), is(1));
        assertThat(listOfStringObjectIds.get(0), is(firstItem.getId().toString()));

        // when
        List<ObjectId> listOfObjectIds = collection.find(new Document("i", 1))
                                                   .map(new Function<Concrete, ObjectId>() {
                                                       @Override
                                                       public ObjectId apply(final Concrete concrete) {
                                                           return concrete.getId();
                                                       }
                                                   })
                                                   .into(new ArrayList<ObjectId>());

        // then
        assertThat(listOfObjectIds.size(), is(1));
        assertThat(listOfObjectIds.get(0), is(firstItem.getId()));
    }

    @Test
    public void testMapReduceWithGenerics() {
        // given
        CodecRegistry codecRegistry = fromProviders(asList(new DocumentCodecProvider(), new NameCodecProvider()));
        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("name", "Pete").append("job", "handyman"),
                                              new Document("name", "Sam").append("job", "Plumber"),
                                              new Document("name", "Pete").append("job", "'electrician'"));

        String mapFunction  = "function(){ emit( this.name , 1 ); }";
        String reduceFunction = "function(key, values){ return values.length; }";
        MongoCollection<Document> collection = database
                .getCollection(getCollectionName())
                .withCodecRegistry(codecRegistry)
                .withReadPreference(ReadPreference.primary())
                .withWriteConcern(WriteConcern.ACKNOWLEDGED);

        // when
        List<Name> result = collection.mapReduce(mapFunction, reduceFunction, Name.class).into(new ArrayList<Name>());

        // then
        assertEquals(new Name("Pete", 2), result.get(0));
        assertEquals(new Name("Sam", 1), result.get(1));
    }

    @Test
    public void testAggregationToACollection() {
        assumeTrue(serverVersionAtLeast(2, 6));

        // given
        List<Document> documents = asList(new Document("_id", 1), new Document("_id", 2));

        getCollectionHelper().insertDocuments(new DocumentCodec(), documents);

        MongoCollection<Document> collection = database
                .getCollection(getCollectionName());

        // when
        List<Document> result = collection.aggregate(Collections.singletonList(new Document("$out", "outCollection")))
                .into(new ArrayList<Document>());

        // then
        assertEquals(documents, result);
    }

    // This is really a test that the default registry created in MongoClient and passed down to MongoCollection has been constructed
    // properly to handle DBRef encoding and decoding
    @Test
    public void testDBRefEncodingAndDecoding() {
        // given
        Document doc = new Document("_id", 1)
                               .append("ref", new DBRef("foo", 5))
                               .append("refWithDB", new DBRef("db", "foo", 5));


        // when
        collection.insertOne(doc);

        // then
        assertEquals(doc, collection.find().first());
    }
}
