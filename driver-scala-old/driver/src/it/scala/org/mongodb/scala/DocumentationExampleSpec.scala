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

import scala.language.reflectiveCalls
import java.util.concurrent.atomic.AtomicBoolean
import com.mongodb.client.model.changestream.{ChangeStreamDocument, FullDocument}

import scala.collection.JavaConverters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.bson.{BsonArray, BsonDocument, BsonNull, BsonString, BsonValue}

// imports required for filters, projections and updates
import org.bson.BsonType

import org.mongodb.scala.model.Aggregates.filter
import org.mongodb.scala.model.Filters.{and, bsonType, elemMatch, exists, gt, in, lt, lte, or}
import org.mongodb.scala.model.Projections.{exclude, excludeId, fields, slice}
import org.mongodb.scala.model.Updates.{combine, currentDate, set}
// end required filters, projections and updates imports


//scalastyle:off magic.number
class DocumentationExampleSpec extends RequiresMongoDBISpec with FuturesSpec {

  // Implicit functions that execute the Observable and return the results
  val waitDuration = Duration(5, "seconds")
  implicit class ObservableExecutor[T](observable: Observable[T]) {
    def execute(): Seq[T] = Await.result(observable, waitDuration)
  }

  implicit class SingleObservableExecutor[T](observable: SingleObservable[T]) {
    def execute(): T = Await.result(observable, waitDuration)
  }
  // end implicit functions

  "The Scala driver" should "be able to insert" in withCollection { collection =>

    // Start Example 1
    collection.insertOne(
      Document("item" -> "canvas", "qty" -> 100, "tags" -> Seq("cotton"), "size" -> Document("h" -> 28, "w" -> 35.5, "uom" -> "cm"))
    ).execute()
    // End Example 1

    // Start Example 2
    val observable = collection.find(equal("item", "canvas"))
    // End Example 2

    observable.execute().size shouldEqual 1

    // Start Example 3
    collection.insertMany(Seq(
      Document("item" -> "journal", "qty" -> 25, "tags" -> Seq("blank", "red"), "size" -> Document("h" -> 14, "w" -> 21, "uom" -> "cm")),
      Document("item" -> "mat", "qty" -> 85, "tags" -> Seq("gray"), "size" -> Document("h" -> 27.9, "w" -> 35.5, "uom" -> "cm")),
      Document("item" -> "mousepad", "qty" -> 25, "tags" -> Seq("gel", "blue"), "size" -> Document("h" -> 19, "w" -> 22.85, "uom" -> "cm"))
    )).execute()
    // End Example 3

    collection.countDocuments().execute() shouldEqual 4
  }

  it should "be able to query top level" in withCollection { collection =>

    // Start Example 6
    collection.insertMany(Seq(
      Document("""{ item: "journal", qty: 25, size: { h: 14, w: 21, uom: "cm" }, status: "A" }"""),
      Document("""{ item: "notebook", qty: 50, size: { h: 8.5, w: 11, uom: "in" }, status: "A" }"""),
      Document("""{ item: "paper", qty: 100, size: { h: 8.5, w: 11, uom: "in" }, status: "D" }"""),
      Document("""{ item: "planner", qty: 75, size: { h: 22.85, w: 30, uom: "cm" }, status: "D" }"""),
      Document("""{ item: "postcard", qty: 45, size: { h: 10, w: 15.25, uom: "cm" }, status: "A" }""")
    )).execute()
    // End Example 6

    collection.countDocuments().execute() shouldEqual 5

    // Start Example 7
    var findObservable = collection.find(Document())
    // End Example 7

    findObservable.execute().size shouldEqual 5

    // Start Example 8
    findObservable = collection.find()
    // End Example 8

    findObservable.execute().size shouldEqual 5

    // Start Example 9
    findObservable = collection.find(equal("status", "D"))
    // End Example 9

    findObservable.execute().size shouldEqual 2

    // Start Example 10
    findObservable = collection.find(in("status", "A", "D"))
    // End Example 10

    findObservable.execute().size shouldEqual 5

    // Start Example 11
    findObservable = collection.find(and(equal("status", "A"), lt("qty", 30)))
    // End Example 11

    findObservable.execute().size shouldEqual 1

    // Start Example 12
    findObservable = collection.find(or(equal("status", "A"), lt("qty", 30)))
    // End Example 12

    findObservable.execute().size shouldEqual 3

    // Start Example 13
    findObservable = collection.find(and(
      equal("status", "A"),
      or(lt("qty", 30), regex("item", "^p")))
    )
    // End Example 13

    findObservable.execute().size shouldEqual 2
  }

  it should "be able to query embedded documents" in withCollection { collection =>

    // Start Example 14
    collection.insertMany(Seq(
      Document("""{ item: "journal", qty: 25, size: { h: 14, w: 21, uom: "cm" }, status: "A" }"""),
      Document("""{ item: "notebook", qty: 50, size: { h: 8.5, w: 11, uom: "in" }, status: "A" }"""),
      Document("""{ item: "paper", qty: 100, size: { h: 8.5, w: 11, uom: "in" }, status: "D" }"""),
      Document("""{ item: "planner", qty: 75, size: { h: 22.85, w: 30, uom: "cm" }, status: "D" }"""),
      Document("""{ item: "postcard", qty: 45, size: { h: 10, w: 15.25, uom: "cm" }, status: "A" }""")
    )).execute()
    // End Example 14

    collection.countDocuments().execute() shouldEqual 5

    // Start Example 15
    var findObservable = collection.find(equal("size", Document("h" -> 14, "w" -> 21, "uom" -> "cm")))
    // End Example 15

    findObservable.execute().size shouldEqual 1

    // Start Example 16
    findObservable = collection.find(equal("size", Document("w" -> 21, "h" -> 14, "uom" -> "cm")))
    // End Example 16

    findObservable.execute().size shouldEqual 0

    // Start Example 17
    findObservable = collection.find(equal("size.uom", "in"))
    // End Example 17

    findObservable.execute().size shouldEqual 2

    // Start Example 18
    findObservable = collection.find(lt("size.h", 15))
    // End Example 18

    findObservable.execute().size shouldEqual 4

    // Start Example 19
    findObservable = collection.find(and(
      lt("size.h", 15),
      equal("size.uom", "in"),
      equal("status", "D")
    ))
    // End Example 19

    findObservable.execute().size shouldEqual 1
  }

  it should "be able to query array" in withCollection { collection =>

    //Start Example 20
    collection.insertMany(Seq(
      Document("""{ item: "journal", qty: 25, tags: ["blank", "red"], dim_cm: [ 14, 21 ] }"""),
      Document("""{ item: "notebook", qty: 50, tags: ["red", "blank"], dim_cm: [ 14, 21 ] }"""),
      Document("""{ item: "paper", qty: 100, tags: ["red", "blank", "plain"], dim_cm: [ 14, 21 ] }"""),
      Document("""{ item: "planner", qty: 75, tags: ["blank", "red"], dim_cm: [ 22.85, 30 ] }"""),
      Document("""{ item: "postcard", qty: 45, tags: ["blue"], dim_cm: [ 10, 15.25 ] }""")
    )).execute()
    //End Example 20

    collection.countDocuments().execute() shouldEqual 5

    //Start Example 21
    var findObservable = collection.find(equal("tags", Seq("red", "blank")))
    //End Example 21

    findObservable.execute().size shouldEqual 1

    //Start Example 22
    findObservable = collection.find(all("tags", "red", "blank"))
    //End Example 22

    findObservable.execute().size shouldEqual 4

    //Start Example 23
    findObservable = collection.find(equal("tags", "red"))
    //End Example 23

    findObservable.execute().size shouldEqual 4

    //Start Example 24
    findObservable = collection.find(gt("dim_cm", 25))
    //End Example 24

    findObservable.execute().size shouldEqual 1

    //Start Example 25
    findObservable = collection.find(and(gt("dim_cm", 15), lt("dim_cm", 20)))
    //End Example 25

    findObservable.execute().size shouldEqual 4

    //Start Example 26
    findObservable = collection.find(elemMatch("dim_cm", Document("$gt" -> 22, "$lt" -> 30)))

    //End Example 26

    findObservable.execute().size shouldEqual 1

    //Start Example 27
    findObservable = collection.find(gt("dim_cm.1", 25))
    //End Example 27

    findObservable.execute().size shouldEqual 1

    //Start Example 28
    findObservable = collection.find(size("tags", 3))
    //End Example 28

    findObservable.execute().size shouldEqual 1
  }

  it should "query array of documents" in withCollection { collection =>

    //Start Example 29
    collection.insertMany(Seq(
      Document("""{ item: "journal", instock: [ { warehouse: "A", qty: 5 }, { warehouse: "C", qty: 15 } ] }"""),
      Document("""{ item: "notebook", instock: [ { warehouse: "C", qty: 5 } ] }"""),
      Document("""{ item: "paper", instock: [ { warehouse: "A", qty: 60 }, { warehouse: "B", qty: 15 } ] }"""),
      Document("""{ item: "planner", instock: [ { warehouse: "A", qty: 40 }, { warehouse: "B", qty: 5 } ] }"""),
      Document("""{ item: "postcard", instock: [ { warehouse: "B", qty: 15 }, { warehouse: "C", qty: 35 } ] }""")
    )).execute()
    //End Example 29

    collection.countDocuments().execute() shouldEqual 5

    //Start Example 30
    var findObservable = collection.find(equal("instock", Document("warehouse" -> "A", "qty" -> 5)))
    //End Example 30

    findObservable.execute().size shouldEqual 1

    //Start Example 31
    findObservable = collection.find(equal("instock", Document("qty" -> 5, "warehouse" -> "A")))
    //End Example 31

    findObservable.execute().size shouldEqual 0

    //Start Example 32
    findObservable = collection.find(lte("instock.0.qty", 20))
    //End Example 32

    findObservable.execute().size shouldEqual 3

    //Start Example 33
    findObservable = collection.find(lte("instock.qty", 20))
    //End Example 33

    findObservable.execute().size shouldEqual 5

    //Start Example 34
    findObservable = collection.find(elemMatch("instock", Document("qty" -> 5, "warehouse" -> "A")))
    //End Example 34

    findObservable.execute().size shouldEqual 1

    //Start Example 35
    findObservable = collection.find(elemMatch("instock", Document("""{ qty: { $gt: 10, $lte: 20 } }""")))
    //End Example 35

    findObservable.execute().size shouldEqual 3

    //Start Example 36
    findObservable = collection.find(and(gt("instock.qty", 10), lte("instock.qty", 20)))
    //End Example 36

    findObservable.execute().size shouldEqual 4

    //Start Example 37
    findObservable = collection.find(and(equal("instock.qty", 5), equal("instock.warehouse", "A")))
    //End Example 37

    findObservable.execute().size shouldEqual 2
  }

  it should "query null and missing fields" in withCollection { collection =>

    //Start Example 38
    collection.insertMany(Seq(
      Document("""{"_id": 1, "item": null}"""),
      Document("""{"_id": 2}""")
    )).execute()
    //End Example 38

    collection.countDocuments().execute() shouldEqual 2

    //Start Example 39
    var findObservable = collection.find(equal("item", BsonNull()))
    //End Example 39

    findObservable.execute().size shouldEqual 2

    //Start Example 40
    findObservable = collection.find(bsonType("item", BsonType.NULL))
    //End Example 40

    findObservable.execute().size shouldEqual 1

    //Start Example 41
    findObservable = collection.find(exists("item", exists = false))
    //End Example 41

    findObservable.execute().size shouldEqual 1
  }

  it should "be able to project fields" in withCollection { collection =>

    //Start Example 42
    collection.insertMany(Seq(
      Document("""{ item: "journal", status: "A", size: { h: 14, w: 21, uom: "cm" }, instock: [ { warehouse: "A", qty: 5 } ] }"""),
      Document("""{ item: "notebook", status: "A",  size: { h: 8.5, w: 11, uom: "in" }, instock: [ { warehouse: "C", qty: 5 } ] }"""),
      Document("""{ item: "paper", status: "D", size: { h: 8.5, w: 11, uom: "in" }, instock: [ { warehouse: "A", qty: 60 } ] }"""),
      Document("""{ item: "planner", status: "D", size: { h: 22.85, w: 30, uom: "cm" }, instock: [ { warehouse: "A", qty: 40 } ] }"""),
      Document("""{ item: "postcard", status: "A", size: { h: 10, w: 15.25, uom: "cm" },
                    instock: [ { warehouse: "B", qty: 15 }, { warehouse: "C", qty: 35 } ] }""")

    )).execute()
    //End Example 42

    collection.countDocuments().execute() shouldEqual 5

    //Start Example 43
    var findObservable = collection.find(equal("status", "A"))
    //End Example 43

    findObservable.execute().size shouldEqual 3

    //Start Example 44
    findObservable = collection.find(equal("status", "A")).projection(include("item", "status"))
    //End Example 44

    findObservable.execute().foreach((doc: Document) => doc.keys should contain only("_id", "item", "status"))

    //Start Example 45
    findObservable = collection.find(equal("status", "A"))
      .projection(fields(include("item", "status"), excludeId()))
    //End Example 45

    findObservable.execute().foreach((doc: Document) => doc.keys should contain only("item", "status"))

    //Start Example 46
    findObservable = collection.find(equal("status", "A")).projection(exclude("item", "status"))
    //End Example 46

    findObservable.execute().foreach((doc: Document) => doc.keys should contain only("_id", "size", "instock"))

    //Start Example 47
    findObservable = collection.find(equal("status", "A")).projection(include("item", "status", "size.uom"))
    //End Example 47

    findObservable.execute().foreach((doc: Document) => {
      doc.keys should contain only("_id", "item", "status", "size")
      doc.get[BsonDocument]("size").get.keys should contain only "uom"
    })

    //Start Example 48
    findObservable = collection.find(equal("status", "A")).projection(exclude("size.uom"))
    //End Example 48

    findObservable.execute().foreach((doc: Document) => {
      doc.keys should contain only("_id", "item", "instock", "status", "size")
      doc.get[BsonDocument]("size").get.keys should contain only("h", "w")
    })

    //Start Example 49
    findObservable = collection.find(equal("status", "A")).projection(include("item", "status", "instock.qty"))
    //End Example 49

    findObservable.execute().foreach((doc: Document) => {
      doc.keys should contain only("_id", "item", "instock", "status")
      doc.get[BsonArray]("instock").get.asScala.foreach((doc: BsonValue) => doc.asInstanceOf[BsonDocument].keys should contain only "qty")
    })

    //Start Example 50
    findObservable = collection.find(equal("status", "A"))
      .projection(fields(include("item", "status"), slice("instock", -1)))
    //End Example 50

    findObservable.execute().foreach((doc: Document) => {
      doc.keys should contain only("_id", "item", "instock", "status")
      doc.get[BsonArray]("instock").get.size() shouldEqual 1
    })
  }

  it should "be able to update" in withCollection { collection =>
    assume(serverVersionAtLeast(List(2, 6, 0)))

    //Start Example 51
    collection.insertMany(Seq(
      Document("""{ item: "canvas", qty: 100, size: { h: 28, w: 35.5, uom: "cm" }, status: "A" }"""),
      Document("""{ item: "journal", qty: 25, size: { h: 14, w: 21, uom: "cm" }, status: "A" }"""),
      Document("""{ item: "mat", qty: 85, size: { h: 27.9, w: 35.5, uom: "cm" }, status: "A" }"""),
      Document("""{ item: "mousepad", qty: 25, size: { h: 19, w: 22.85, uom: "cm" }, status: "P" }"""),
      Document("""{ item: "notebook", qty: 50, size: { h: 8.5, w: 11, uom: "in" }, status: "P" }"""),
      Document("""{ item: "paper", qty: 100, size: { h: 8.5, w: 11, uom: "in" }, status: "D" }"""),
      Document("""{ item: "planner", qty: 75, size: { h: 22.85, w: 30, uom: "cm" }, status: "D" }"""),
      Document("""{ item: "postcard", qty: 45, size: { h: 10, w: 15.25, uom: "cm" }, status: "A" }"""),
      Document("""{ item: "sketchbook", qty: 80, size: { h: 14, w: 21, uom: "cm" }, status: "A" }"""),
      Document("""{ item: "sketch pad", qty: 95, size: { h: 22.85, w: 30.5, uom: "cm" }, status: "A" }""")
    )).execute()
    //End Example 51

    collection.countDocuments().execute() shouldEqual 10

    //Start Example 52
    collection.updateOne(equal("item", "paper"),
      combine(set("size.uom", "cm"), set("status", "P"), currentDate("lastModified"))
    ).execute()
    //End Example 52

    collection.find(equal("item", "paper")).execute().foreach((doc: Document) => {
      doc.get[BsonDocument]("size").get.get("uom") shouldEqual BsonString("cm")
      doc.get[BsonString]("status").get shouldEqual BsonString("P")
      doc.containsKey("lastModified") shouldBe true
    })

    //Start Example 53
    collection.updateMany(lt("qty", 50),
      combine(set("size.uom", "in"), set("status", "P"), currentDate("lastModified"))
    ).execute()
    //End Example 53

    collection.find(lt("qty", 50)).execute().foreach((doc: Document) => {
      doc.get[BsonDocument]("size").get.get("uom") shouldEqual BsonString("in")
      doc.get[BsonString]("status").get shouldEqual BsonString("P")
      doc.containsKey("lastModified") shouldBe true
    })

    //Start Example 54
    collection.replaceOne(equal("item", "paper"),
      Document("""{ item: "paper", instock: [ { warehouse: "A", qty: 60 }, { warehouse: "B", qty: 40 } ] }""")
    ).execute()
    //End Example 54

    collection.find(equal("item", "paper")).projection(excludeId()).execute().foreach((doc: Document) =>
      doc shouldEqual Document("""{ item: "paper", instock: [ { warehouse: "A", qty: 60 }, { warehouse: "B", qty: 40 } ] }""")
    )
  }

  it should "be able to delete" in withCollection { collection =>

    //Start Example 55
    collection.insertMany(Seq(
      Document("""{ item: "journal", qty: 25, size: { h: 14, w: 21, uom: "cm" }, status: "A" }"""),
      Document("""{ item: "notebook", qty: 50, size: { h: 8.5, w: 11, uom: "in" }, status: "A" }"""),
      Document("""{ item: "paper", qty: 100, size: { h: 8.5, w: 11, uom: "in" }, status: "D" }"""),
      Document("""{ item: "planner", qty: 75, size: { h: 22.85, w: 30, uom: "cm" }, status: "D" }"""),
      Document("""{ item: "postcard", qty: 45, size: { h: 10, w: 15.25, uom: "cm" }, status: "A" }""")
    )).execute()
    //End Example 55

    collection.countDocuments().execute() shouldEqual 5

    //Start Example 57
    collection.deleteMany(equal("status", "A")).execute()
    //End Example 57

    collection.countDocuments().execute() shouldEqual 2

    //Start Example 58
    collection.deleteOne(equal("status", "D")).execute()
    //End Example 58

    collection.countDocuments().execute() shouldEqual 1

    //Start Example 56
    collection.deleteMany(Document()).execute()
    //End Example 56

    collection.countDocuments().execute() shouldEqual 0
  }

  it should "be able to watch" in withCollection { collection =>
    assume(serverVersionAtLeast(List(3, 6, 0)) && !hasSingleHost())
    val inventory: MongoCollection[Document] = collection
    val stop: AtomicBoolean = new AtomicBoolean(false)
    new Thread(new Runnable {
      override def run(): Unit = {
        while (!stop.get) {
          collection.insertOne(Document())
          try {
            Thread.sleep(10)
          } catch {
            case e: InterruptedException =>
            // ignore
          }
        }
      }
    }).start()

    val observer = new Observer[ChangeStreamDocument[Document]] {
      def getResumeToken: BsonDocument = Document().underlying
      override def onNext(result: ChangeStreamDocument[Document]): Unit = {}
      override def onError(e: Throwable): Unit = {}
      override def onComplete(): Unit = {}
    }

    // Start Changestream Example 1
    var observable: ChangeStreamObservable[Document] = inventory.watch()
    observable.subscribe(observer)
    // End Changestream Example 1

    // Start Changestream Example 2
    observable = inventory.watch.fullDocument(FullDocument.UPDATE_LOOKUP)
    observable.subscribe(observer)
    // End Changestream Example 2

    // Start Changestream Example 3
    val resumeToken: BsonDocument = observer.getResumeToken
    observable = inventory.watch.resumeAfter(resumeToken)
    observable.subscribe(observer)
    // End Changestream Example 3

    // Start Changestream Example 4
    val pipeline: List[Bson] = List(filter(or(Document("{'fullDocument.username': 'alice'}"), in("operationType", List("delete")))))
    observable = inventory.watch(pipeline)
    observable.subscribe(observer)
    // End Changestream Example 4

    stop.set(true)
  }

  // Matcher Trait overrides...
  def equal[TItem](fieldName: String, value: TItem): Bson = org.mongodb.scala.model.Filters.equal(fieldName, value)
  def regex(fieldName: String, pattern: String): Bson = org.mongodb.scala.model.Filters.regex(fieldName, pattern)
  def all[TItem](fieldName: String, values: TItem*): Bson =  org.mongodb.scala.model.Filters.all(fieldName, values: _*)
  def size(fieldName: String, size: Int): Bson = org.mongodb.scala.model.Filters.size(fieldName, size)
  def include(fieldNames: String*): Bson = org.mongodb.scala.model.Projections.include(fieldNames: _*)

}
