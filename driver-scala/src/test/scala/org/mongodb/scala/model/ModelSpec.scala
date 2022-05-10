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

package org.mongodb.scala.model

import org.mongodb.scala._

class ModelSpec extends BaseSpec {

  it should "be able to create CountOptions" in {
    val options = CountOptions()
    options shouldBe a[com.mongodb.client.model.CountOptions]
  }

  it should "be able to create CreateCollectionOptions" in {
    val options = CreateCollectionOptions()
    options shouldBe a[com.mongodb.client.model.CreateCollectionOptions]
  }

  it should "be able to create FindOneAndDeleteOptions" in {
    val options = FindOneAndDeleteOptions()
    options shouldBe a[com.mongodb.client.model.FindOneAndDeleteOptions]
  }

  it should "be able to create FindOneAndReplaceOptions" in {
    val options = FindOneAndReplaceOptions()
    options shouldBe a[com.mongodb.client.model.FindOneAndReplaceOptions]
  }

  it should "be able to create FindOneAndUpdateOptions" in {
    val options = FindOneAndUpdateOptions()
    options shouldBe a[com.mongodb.client.model.FindOneAndUpdateOptions]
  }

  it should "be able to create IndexOptions" in {
    val options = IndexOptions()
    options shouldBe a[com.mongodb.client.model.IndexOptions]
  }

  it should "be able to create InsertManyOptions" in {
    val options = InsertManyOptions()
    options shouldBe a[com.mongodb.client.model.InsertManyOptions]
  }

  it should "be able to create RenameCollectionOptions" in {
    val options = RenameCollectionOptions()
    options shouldBe a[com.mongodb.client.model.RenameCollectionOptions]
  }

  it should "be able to create UpdateOptions" in {
    val options = UpdateOptions()
    options shouldBe a[com.mongodb.client.model.UpdateOptions]
  }

  it should "be able to create IndexModel" in {
    val model = IndexModel(Document("a" -> 1))
    model shouldBe a[com.mongodb.client.model.IndexModel]

    val model2 = IndexModel(Document("a" -> 1), IndexOptions())
    model2 shouldBe a[com.mongodb.client.model.IndexModel]
  }

  it should "be able to create DeleteManyModel" in {
    val model = DeleteManyModel(Document("a" -> 1))
    model shouldBe a[com.mongodb.client.model.DeleteManyModel[_]]
  }

  it should "be able to create DeleteOneModel" in {
    val model = DeleteOneModel(Document("a" -> 1))
    model shouldBe a[com.mongodb.client.model.DeleteOneModel[_]]
  }

  it should "be able to create InsertOneModel" in {
    val model = InsertOneModel(Document("a" -> 1))
    model shouldBe a[com.mongodb.client.model.InsertOneModel[_]]
  }

  it should "be able to create ReplaceOneModel" in {
    val model = ReplaceOneModel(Document("a" -> 1), Document("a" -> 2))
    model shouldBe a[com.mongodb.client.model.ReplaceOneModel[_]]
  }

  it should "be able to create UpdateManyModel" in {
    val model = UpdateManyModel(Document("a" -> 1), Document("$set" -> Document("a" -> 2)))
    model shouldBe a[com.mongodb.client.model.UpdateManyModel[_]]

    val model2 = UpdateManyModel(Document("a" -> 1), Document("$set" -> Document("a" -> 2)), UpdateOptions())
    model2 shouldBe a[com.mongodb.client.model.UpdateManyModel[_]]

    val model3 = UpdateManyModel(Document("a" -> 1), Seq(Document("$set" -> Document("a" -> 2))))
    model3 shouldBe a[com.mongodb.client.model.UpdateManyModel[_]]

    val model4 = UpdateManyModel(Document("a" -> 1), Seq(Document("$set" -> Document("a" -> 2))), UpdateOptions())
    model4 shouldBe a[com.mongodb.client.model.UpdateManyModel[_]]
  }

  it should "be able to create UpdateOneModel" in {
    val model = UpdateOneModel(Document("a" -> 1), Document("$set" -> Document("a" -> 2)))
    model shouldBe a[com.mongodb.client.model.UpdateOneModel[_]]

    val model2 = UpdateOneModel(Document("a" -> 1), Document("$set" -> Document("a" -> 2)), UpdateOptions())
    model2 shouldBe a[com.mongodb.client.model.UpdateOneModel[_]]

    val model3 = UpdateOneModel(Document("a" -> 1), Seq(Document("$set" -> Document("a" -> 2))))
    model3 shouldBe a[com.mongodb.client.model.UpdateOneModel[_]]

    val model4 = UpdateOneModel(Document("a" -> 1), Seq(Document("$set" -> Document("a" -> 2))), UpdateOptions())
    model4 shouldBe a[com.mongodb.client.model.UpdateOneModel[_]]
  }

  it should "be able to create BsonField" in {
    val bsonField = BsonField("key", Document("a" -> 1))
    bsonField shouldBe a[com.mongodb.client.model.BsonField]
  }

}
