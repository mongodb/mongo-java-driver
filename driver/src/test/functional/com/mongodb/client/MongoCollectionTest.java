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

import com.mongodb.Function;
import com.mongodb.client.model.FindOptions;
import com.mongodb.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.Document;

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
        List<CodecProvider> codecs = Arrays.asList(new DocumentCodecProvider(), new ConcreteCodecProvider());
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
        List<CodecProvider> codecs = Arrays.asList(new DocumentCodecProvider(), new ConcreteCodecProvider());
        MongoCollectionOptions options =
                MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(codecs)).build();
        MongoCollection<Concrete> concreteCollection = database.getCollection(getCollectionName(), Concrete.class, options);

        Concrete firstItem = new Concrete("first", 1, 2L, 3.0, 5L);
        concreteCollection.insertOne(firstItem);

        Concrete secondItem = new Concrete("second", 7, 11L, 13.0, 17L);
        concreteCollection.insertOne(secondItem);

        // when
        List<String> listOfStringObjectIds = concreteCollection.find(new FindOptions().criteria(new Document("i", 1)))
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
        List<ObjectId> listOfObjectIds = concreteCollection.find(new FindOptions().criteria(new Document("i", 1)))
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
}
