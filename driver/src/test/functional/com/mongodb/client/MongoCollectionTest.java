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
import com.mongodb.client.model.FindOptions;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class MongoCollectionTest extends DatabaseTestCase {

    @Test
    public void testFindAndUpdateWithGenerics() {
        List<CodecProvider> codecs = Arrays.asList(new ValueCodecProvider(),
                                                   new DocumentCodecProvider(),
                                                   new ConcreteCodecProvider());
        MongoCollectionOptions options =
                MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(codecs)).build();
        MongoCollection<Concrete> collection = database.getCollection(getCollectionName(), Concrete.class, options);

        Concrete doc = new Concrete(new ObjectId(), "str", 5, 10L, 4.0, 3290482390480L);
        collection.insertOne(doc);

        Concrete newDoc = collection.findOneAndUpdate(new Document("i", 5), new Document("$set", new Document("i", 6)));

        assertNotNull(newDoc);
        assertEquals(doc, newDoc);
    }

    @Test
    public void shouldBeAbleToQueryTypedCollectionAndMapResultsIntoTypedLists() {
        // given
        List<CodecProvider> codecs = Arrays.asList(new ValueCodecProvider(),
                                                   new DocumentCodecProvider(),
                                                   new ConcreteCodecProvider());
        MongoCollectionOptions options =
                MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(codecs)).build();
        MongoCollection<Concrete> concreteCollection = database.getCollection(getCollectionName(), Concrete.class, options);

        Concrete firstItem = new Concrete("first", 1, 2L, 3.0, 5L);
        concreteCollection.insertOne(firstItem);

        Concrete secondItem = new Concrete("second", 7, 11L, 13.0, 17L);
        concreteCollection.insertOne(secondItem);

        // when
        List<String> listOfStringObjectIds = concreteCollection.find(new Document("i", 1), new FindOptions())
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
        List<ObjectId> listOfObjectIds = concreteCollection.find(new Document("i", 1), new FindOptions())
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
        List<CodecProvider> codecs = Arrays.asList(new DocumentCodecProvider(), new NameCodecProvider());
        MongoCollectionOptions options =
            MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(codecs)).build();

        getCollectionHelper().insertDocuments(new DocumentCodec(), new Document("name", "Pete").append("job", "handyman"),
                                              new Document("name", "Sam").append("job", "Plumber"),
                                              new Document("name", "Pete").append("job", "'electrician'"));

        String mapFunction  = "function(){ emit( this.name , 1 ); }";
        String reduceFunction = "function(key, values){ return values.length; }";
        MongoCollection<Document> collection = database.getCollection(getCollectionName(), Document.class, options);

        // when
        List<Name> result = collection.mapReduce(mapFunction, reduceFunction, Name.class).into(new ArrayList<Name>());

        // then
        assertEquals(new Name("Pete", 2), result.get(0));
        assertEquals(new Name("Sam", 1), result.get(1));
    }

    // This is really a test that the default registry created in MongoClient and passed down to MongoCollection has been constructed
    // properly to handle DBRef encoding and decoding
    @Test
    public void testDBRefEncodingAndDecoding() {
        // given
        Document doc = new Document("_id", 1).append("ref", new DBRef("foo", 5));

        // when
        collection.insertOne(doc);

        // then
        assertEquals(doc, collection.find().first());
    }
}
