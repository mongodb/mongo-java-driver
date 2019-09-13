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

package org.mongodb.scala.bson.collections

import org.mongodb.scala.bson._
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.collection.mutable
import org.scalatest.{ FlatSpec, Matchers }

class DocumentImplicitTypeConversion extends FlatSpec with Matchers {

  val emptyDoc: Document = Document.empty

  "Document additions and updates" should "support simple additions" in {
    val doc1: Document = Document() + ("key" -> "value")
    doc1 should equal(Document("key" -> BsonString("value")))

    val doc2: Document = doc1 + ("key2" -> 2)
    doc2 should equal(Document("key" -> BsonString("value"), "key2" -> BsonInt32(2)))
  }

  it should "support multiple additions" in {
    val doc1: Document = emptyDoc + ("key" -> "value", "key2" -> 2, "key3" -> true, "key4" -> None)
    doc1 should equal(
      Document("key" -> BsonString("value"), "key2" -> BsonInt32(2), "key3" -> BsonBoolean(true), "key4" -> BsonNull())
    )
  }

  it should "support addition of a traversable" in {
    val doc1: Document = emptyDoc ++ Document("key" -> "value", "key2" -> 2, "key3" -> true, "key4" -> None)
    doc1 should equal(
      Document("key" -> BsonString("value"), "key2" -> BsonInt32(2), "key3" -> BsonBoolean(true), "key4" -> BsonNull())
    )
  }

  it should "support updated" in {
    val doc1: Document = emptyDoc.updated("key", "value")
    emptyDoc should not be doc1
    doc1 should equal(Document("key" -> BsonString("value")))
  }

  it should "be creatable from mixed types" in {
    val doc1: Document = Document(
      "a" -> "string",
      "b" -> true,
      "c" -> List("a", "b", "c"),
      "d" -> Document("a" -> "string", "b" -> true, "c" -> List("a", "b", "c"))
    )

    val doc2: mutable.Document = mutable.Document(
      "a" -> "string",
      "b" -> true,
      "c" -> List("a", "b", "c"),
      "d" ->
        mutable.Document("a" -> "string", "b" -> true, "c" -> List("a", "b", "c"))
    )
    doc1.toBsonDocument should equal(doc2.toBsonDocument)
  }
}
