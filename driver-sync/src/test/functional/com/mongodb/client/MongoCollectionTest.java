/*
 * Copyright 2008-present MongoDB, Inc.
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
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.result.InsertManyResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.codecs.pojo.entities.conventions.BsonRepresentationModel;
import org.bson.json.JsonObject;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.isServerlessTest;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

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
    public void testFindOneAndUpdateEmpty() {
        boolean exceptionFound = false;
        collection.insertOne(new Document().append("_id", "fakeId").append("one", 1).append("foo", "bar"));

        try {
            collection.findOneAndUpdate(new Document(), new Document());
        } catch (IllegalArgumentException e) {
            assertEquals("Invalid BSON document for an update. The document may not be empty.", e.getMessage());
            exceptionFound = true;
        }
        assertTrue(exceptionFound);
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
                                                       .map(concrete -> concrete.getId())
                                                       .map(objectId -> objectId.toString()).into(new ArrayList<>());

        // then
        assertThat(listOfStringObjectIds.size(), is(1));
        assertThat(listOfStringObjectIds.get(0), is(firstItem.getId().toString()));

        // when
        List<ObjectId> listOfObjectIds = collection.find(new Document("i", 1))
                                                   .map(concrete -> concrete.getId())
                                                   .into(new ArrayList<>());

        // then
        assertThat(listOfObjectIds.size(), is(1));
        assertThat(listOfObjectIds.get(0), is(firstItem.getId()));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testMapReduceWithGenerics() {
        assumeFalse(isServerlessTest());

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
        List<Name> result = collection.mapReduce(mapFunction, reduceFunction, Name.class).into(new ArrayList<>());

        // then
        assertTrue(result.contains(new Name("Pete", 2)));
        assertTrue(result.contains(new Name("Sam", 1)));
    }

    @Test
    public void testAggregationToACollection() {
        assumeFalse(isServerlessTest());

        // given
        List<Document> documents = asList(new Document("_id", 1), new Document("_id", 2));

        getCollectionHelper().insertDocuments(new DocumentCodec(), documents);

        MongoCollection<Document> collection = database
                .getCollection(getCollectionName());

        // when
        List<Document> result = collection.aggregate(Collections.singletonList(new Document("$out", "outCollection")))
                .into(new ArrayList<>());

        // then
        assertEquals(documents, result);
    }

    @Test
    public void bulkInsertRawBsonDocuments() {
        // given
        List<RawBsonDocument> docs = asList(RawBsonDocument.parse("{a: 1}"), RawBsonDocument.parse("{a: 2}"));

        // when
        InsertManyResult result = collection.withDocumentClass(RawBsonDocument.class).insertMany(docs);

        // then
        Map<Integer, BsonValue> expectedResult = new HashMap<>();
        expectedResult.put(0, null);
        expectedResult.put(1, null);
        assertEquals(expectedResult, result.getInsertedIds());
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

    @Test
    public void testJsonObjectEncodingAndDecoding() {
        // given
        MongoCollection<JsonObject> test = database.getCollection("test", JsonObject.class);
        JsonObject json = new JsonObject("{\"_id\": {\"$oid\": \"5f5a5442306e56d34136dbcf\"}, \"hello\": 1}");
        test.drop();

        // when
        test.insertOne(json);

        // then
        assertEquals(json, test.find().first());
    }

    @Test
    public void testObjectIdToStringConversion() {
        // given
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoCollection<BsonRepresentationModel> test =
                database.getCollection("test", BsonRepresentationModel.class)
                .withCodecRegistry(pojoCodecRegistry);
        test.drop();

        // when
        test.insertOne(new BsonRepresentationModel(null, 1));

        // then
        assertNotNull(test.find().first().getId());
    }
}
