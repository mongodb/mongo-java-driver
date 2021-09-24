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

import org.mongodb.scala.bson.codecs.Macros.createCodecProvider
import org.bson.codecs.configuration.CodecRegistries.{ fromProviders, fromRegistries }
import org.bson.codecs.configuration.CodecRegistry
import org.bson.conversions.Bson
import org.mongodb.scala.model.Indexes

class MongoCollectionCaseClassSpec extends RequiresMongoDBISpec with FuturesSpec {

  case class Contact(phone: String)
  case class User(_id: Int, username: String, age: Int, hobbies: List[String], contacts: List[Contact])
  case class Optional(_id: Int, optional: Option[Int])

  val codecRegistry: CodecRegistry = fromRegistries(
    fromProviders(classOf[User], classOf[Contact], classOf[Optional]),
    MongoClient.DEFAULT_CODEC_REGISTRY
  )

  "The Scala driver" should "handle case classes" in withDatabase(databaseName) { database =>
    val collection = database.getCollection[User](collectionName).withCodecRegistry(codecRegistry)

    val user = User(
      _id = 1,
      age = 30,
      username = "Bob",
      hobbies = List[String]("hiking", "music"),
      contacts = List(Contact("123 12314"), Contact("234 234234"))
    )
    collection.insertOne(user).futureValue

    info("The collection should have the expected document")
    val expectedDocument = Document("""{_id: 1, age: 30, username: "Bob", hobbies: ["hiking", "music"],
          | contacts: [{phone: "123 12314"}, {phone: "234 234234"}]}""".stripMargin)
    collection.find[Document]().first().futureValue should equal(expectedDocument)

    info("The collection should find and return the user")
    collection.find().first().futureValue should equal(user)
  }

  it should "handle optional values" in withDatabase(databaseName) { database =>
    val collection = database.getCollection[Optional](collectionName).withCodecRegistry(codecRegistry)

    val none = Optional(_id = 1, None)
    collection.insertOne(none).futureValue

    info("The collection should have the expected document")
    val expectedDocument = Document("{_id: 1, optional: null}")
    collection.find[Document]().first().futureValue should equal(expectedDocument)

    info("The collection should find and return the optional")
    collection.find().first().futureValue should equal(none)

    collection.drop().futureValue

    val some = Optional(_id = 1, Some(1))
    collection.insertOne(some).futureValue

    info("The collection should find and return the optional")
    collection.find().first().futureValue should equal(some)
  }

  it should "handle converting to case classes where there is extra data" in withDatabase(databaseName) { database =>
    val collection = database.getCollection[Contact](collectionName).withCodecRegistry(codecRegistry)

    database
      .getCollection(collectionName)
      .insertOne(Document("""{_id: 5, phone: "555 232323", active: true}"""))
      .futureValue
    val contact = Contact("555 232323")
    collection.find().first().futureValue should equal(contact)
  }

  it should "allow composing operations that return Publisher[Void] from the underlying java driver with other operations" in
    withDatabase(databaseName) { database =>
      val collection = database.getCollection[Contact](collectionName).withCodecRegistry(codecRegistry)
      val indexKey = "phone"
      val index: Bson = Indexes.ascending(indexKey)

      def dropIndex(indexName: String): SingleObservable[Unit] =
        collection
          .dropIndex(indexName)

      def createIndex(index: Bson): SingleObservable[String] =
        collection
          .createIndex(index)

      val resultObservable: Observable[String] = for {
        createdIndex <- createIndex(index)
        _ <- dropIndex(createdIndex)
        createdAgain <- createIndex(index)
      } yield createdAgain
      val result = resultObservable.head().futureValue

      assert(result === s"${indexKey}_1")

    }

}
