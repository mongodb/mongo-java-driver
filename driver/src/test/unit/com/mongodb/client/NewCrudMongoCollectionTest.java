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

import com.mongodb.ExplainVerbosity;
import com.mongodb.MongoCursor;
import com.mongodb.client.model.BulkWriteModel;
import com.mongodb.client.model.CountModel;
import com.mongodb.client.model.DistinctModel;
import com.mongodb.client.model.FindModel;
import com.mongodb.client.model.InsertManyModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.RemoveOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.RemoveResult;
import com.mongodb.client.result.ReplaceOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.mongodb.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NewCrudMongoCollectionTest {

    public void tryNewMongoCollection(NewMongoCollection<Animal> animals) {
        // writes
        InsertOneResult insertOneResult = animals.insertOne(new Mammal());
        System.out.println(insertOneResult.getInsertedCount());
        System.out.println(insertOneResult.getInsertedId());

        animals.insertOne(new Insect());

        InsertManyResult insertManyResult = animals.insertMany(Arrays.<Animal>asList(new Mammal(), new Insect()));
        System.out.println(insertManyResult.getInsertedCount());
        System.out.println(insertManyResult.getInsertedIds());

        animals.insertMany(Arrays.<Mammal>asList(new Mammal(), new Mammal()));

        animals.insertMany(new InsertManyModel<Animal>(Arrays.asList(new Mammal(), new Mammal()))
                           .ordered(false));

        ReplaceOneResult replaceOneResult = animals.replaceOne(new Document("_id", 1),
                                                               new Insect());
        animals.replaceOne(new ReplaceOneModel<Animal>(new Document("_id", 1),
                                                       new Insect())
                           .upsert(true));

        UpdateResult updateOneResult = animals.updateOne(new Document("_id", 1),
                                                         new Document("$set", new Document("color", "blue")));
        updateOneResult = animals.updateOne(new UpdateOneModel<Animal>(new Document("_id", 1),
                                                                       new Document("$set", new Document("color", "blue")))
                                            .upsert(true));

        UpdateResult updateManyResult = animals.updateMany(new Document("type", "Mammal"),
                                                           new Document("$set", new Document("warm-blooded", true)));
        animals.updateMany(new UpdateManyModel<Animal>(new Document("type", "Mammal"),
                                                       new Document("$set", new Document("warm-blooded", true)))
                           .upsert(true));

        RemoveResult removeOneResult = animals.removeOne(new Document("_id", 1));
        RemoveResult removeManyResult = animals.removeMany(new Document("type", "Mammal"));

        // reads
        long count = animals.count();
        count = animals.count(new CountModel().filter(new Document("type", "Mammal")));

        MongoCursor<BsonValue> distinctValues = animals.distinct("type");
        distinctValues = animals.distinct(new DistinctModel("type")
                                          .filter(new Document("color", "blue")));

        MongoCursor<Animal> animalCursor = animals.find(new Document("type", "Mammal"));
        animalCursor = animals.find(new FindModel(new Document("type", "Mammal"))
                                    .projection(new Document("_id", 1)));

        // bulk

        List<WriteModel<? extends Animal>> writes = new ArrayList<WriteModel<? extends Animal>>();
        writes.add(new InsertOneModel<Mammal>(new Mammal()));
        writes.add(new UpdateOneModel<Animal>(new Document("_id", 1),
                                              new Document("$set", new Document("color", "blue")))
                   .upsert(true));
        writes.add(new ReplaceOneModel<Insect>(new Document("_id", 1),
                                               new Insect())
                   .upsert(true));
        writes.add(new RemoveOneModel<Animal>(new Document("_id", 1)));

        animals.bulkWrite(writes);


        BulkWriteModel<Animal> bulkWriteModel = new BulkWriteModel<Animal>(writes);

        animals.bulkWrite(bulkWriteModel
                          .ordered(false));

        Document explainPlan = animals.explain(new FindModel(new Document("type", "Mammal")),
                                               ExplainVerbosity.QUERY_PLANNER);
        explainPlan = animals.explain(new CountModel().filter(new Document("type", "Mammal")),
                                      ExplainVerbosity.QUERY_PLANNER);

    }

}


abstract class Animal {

}

class Mammal extends Animal {

}

class Insect extends Animal {

}
