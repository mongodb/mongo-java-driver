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

package org.mongodb.scala

import org.bson.BsonString

class SmokeTestISpec extends RequiresMongoDBISpec with FuturesSpec {

  "The Scala driver" should "handle common scenarios without error" in withDatabase(databaseName) { database =>
    val client = mongoClient()
    val document = Document("_id" -> 1)
    val updatedDocument = Document("_id" -> 1, "a" -> 1)

    client.listDatabaseNames().futureValue

    info("Creating a collection")
    database.createCollection(collectionName).futureValue

    info("Database names should include the new collection")
    val updatedNames = client.listDatabaseNames().futureValue
    updatedNames should contain(databaseName)

    info("The collection name should be in the collection names list")
    val collectionNames = database.listCollectionNames().futureValue
    collectionNames should contain(collectionName)

    info("The collection should be empty")
    val collection = database.getCollection(collectionName)
    collection.countDocuments().futureValue shouldBe 0

    info("find first should return null if no documents")
    collection.find().first().futureValue shouldBe null // scalastyle:ignore

    info("find should return an empty list")
    collection.find().futureValue shouldBe empty

    info("Insert a document")
    collection.insertOne(document).futureValue

    info("The count should be one")
    collection.countDocuments().futureValue shouldBe 1

    info("the find that document")
    collection.find().futureValue.head shouldBe document

    info("update that document")
    collection.updateOne(document, Document("$set" -> Document("a" -> 1))).futureValue.wasAcknowledged shouldBe true

    info("the find the updated document")
    collection.find().first().futureValue shouldBe updatedDocument

    info("aggregate the collection")
    collection.aggregate(List(Document("$match" -> Document("a" -> 1)))).futureValue.head shouldBe updatedDocument

    info("remove all documents")
    collection.deleteOne(Document()).futureValue.getDeletedCount() shouldBe 1

    info("The count is zero")
    collection.countDocuments().futureValue shouldBe 0

    info("create an index")
    collection.createIndex(Document("test" -> 1)).futureValue shouldBe "test_1"

    info("has the newly created index")
    val indexNames = collection.listIndexes().futureValue.map(doc => doc[BsonString]("name")).map(name => name.getValue)
    indexNames should contain("_id_")
    indexNames should contain("test_1")

    info("drop the index")
    collection.dropIndex("test_1").futureValue

    info("has a single index left '_id'")
    collection.listIndexes.futureValue.length shouldBe 1

    info("can rename the collection")
    val newCollectionName = "new" + collectionName.capitalize
    collection.renameCollection(MongoNamespace(databaseName, newCollectionName)).futureValue

    info("the new collection name is in the collection names list")
    database.listCollectionNames().futureValue should contain(newCollectionName)

    info("drop the new collection")
    val newCollection = database.getCollection(newCollectionName)
    newCollection.drop().futureValue

    info("there are no indexes")
    newCollection.listIndexes().futureValue.length shouldBe 0

    info("the new collection name is no longer in the collection names list")
    database.listCollectionNames().futureValue should not contain (newCollectionName)
  }
}
