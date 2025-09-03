package org.mongodb.scala.model.bulk

import org.mongodb.scala.bson.Document
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.{ BaseSpec, MongoNamespace }

class BulkModelSpec extends BaseSpec {

  val namespace = new MongoNamespace("db.coll")
  val filter: Bson = Document("a" -> 1)
  val update: Bson = Document("$set" -> Document("b" -> 2))
  val replacement = Document("b" -> 2)
  val document = Document("a" -> 1)
  val updatePipeline: Seq[Document] = Seq(Document("$set" -> Document("b" -> 2)))

  it should "be able to create ClientNamespacedInsertOneModel" in {
    val insertOneModel = ClientNamespacedWriteModel.insertOne(namespace, document)
    insertOneModel shouldBe a[ClientNamespacedInsertOneModel]
    insertOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedInsertOneModel]
  }

  it should "be able to create ClientNamespacedUpdateOneModel with filter and update" in {
    val updateOneModel = ClientNamespacedWriteModel.updateOne(namespace, filter, update)
    updateOneModel shouldBe a[ClientNamespacedUpdateOneModel]
    updateOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedUpdateOneModel]
  }

  it should "be able to create ClientNamespacedUpdateOneModel with filter, update, and options" in {
    val options = ClientUpdateOneOptions.clientUpdateOneOptions()
    val updateOneModel = ClientNamespacedWriteModel.updateOne(namespace, filter, update, options)
    updateOneModel shouldBe a[ClientNamespacedUpdateOneModel]
    updateOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedUpdateOneModel]
  }

  it should "be able to create ClientNamespacedUpdateOneModel with update pipeline" in {
    val updateOneModel = ClientNamespacedWriteModel.updateOne(namespace, filter, updatePipeline)
    updateOneModel shouldBe a[ClientNamespacedUpdateOneModel]
    updateOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedUpdateOneModel]
  }

  it should "be able to create ClientNamespacedUpdateOneModel with update pipeline and options" in {
    val options = ClientUpdateOneOptions.clientUpdateOneOptions()
    val updateOneModel = ClientNamespacedWriteModel.updateOne(namespace, filter, updatePipeline, options)
    updateOneModel shouldBe a[ClientNamespacedUpdateOneModel]
    updateOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedUpdateOneModel]
  }

  it should "be able to create ClientNamespacedUpdateManyModel with filter and update" in {
    val updateManyModel = ClientNamespacedWriteModel.updateMany(namespace, filter, update)
    updateManyModel shouldBe a[ClientNamespacedUpdateManyModel]
    updateManyModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedUpdateManyModel]
  }
  it should "be able to create ClientNamespacedUpdateManyModel with filter, update and options" in {
    val options = ClientUpdateManyOptions.clientUpdateManyOptions()
    val updateManyModel = ClientNamespacedWriteModel.updateMany(namespace, filter, update, options)
    updateManyModel shouldBe a[ClientNamespacedUpdateManyModel]
    updateManyModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedUpdateManyModel]
  }

  it should "be able to create ClientNamespacedUpdateManyModel with filter, updatePipeline" in {
    val updateManyModel = ClientNamespacedWriteModel.updateMany(namespace, filter, updatePipeline)
    updateManyModel shouldBe a[ClientNamespacedUpdateManyModel]
    updateManyModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedUpdateManyModel]
  }

  it should "be able to create ClientNamespacedUpdateManyModel with filter, updatePipeline and options" in {
    val options = ClientUpdateManyOptions.clientUpdateManyOptions()
    val updateManyModel = ClientNamespacedWriteModel.updateMany(namespace, filter, updatePipeline, options)
    updateManyModel shouldBe a[ClientNamespacedUpdateManyModel]
    updateManyModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedUpdateManyModel]
  }

  it should "be able to create ClientNamespacedReplaceOneModel" in {
    val replaceOneModel = ClientNamespacedWriteModel.replaceOne(namespace, filter, replacement)
    replaceOneModel shouldBe a[ClientNamespacedReplaceOneModel]
    replaceOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedReplaceOneModel]
  }

  it should "be able to create ClientNamespacedReplaceOneModel with options" in {
    val options = ClientReplaceOneOptions.clientReplaceOneOptions()
    val replaceOneModel = ClientNamespacedWriteModel.replaceOne(namespace, filter, replacement, options)
    replaceOneModel shouldBe a[ClientNamespacedReplaceOneModel]
    replaceOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedReplaceOneModel]
  }

  it should "be able to create ClientNamespacedDeleteOneModel" in {
    val deleteOneModel = ClientNamespacedWriteModel.deleteOne(namespace, filter)
    deleteOneModel shouldBe a[ClientNamespacedDeleteOneModel]
    deleteOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedDeleteOneModel]
  }

  it should "be able to create ClientNamespacedDeleteOneModel with options" in {
    val options = ClientDeleteOneOptions.clientDeleteOneOptions()
    val deleteOneModel = ClientNamespacedWriteModel.deleteOne(namespace, filter, options)
    deleteOneModel shouldBe a[ClientNamespacedDeleteOneModel]
    deleteOneModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedDeleteOneModel]
  }

  it should "be able to create ClientNamespacedDeleteManyModel" in {
    val deleteManyModel = ClientNamespacedWriteModel.deleteMany(namespace, filter)
    deleteManyModel shouldBe a[ClientNamespacedDeleteManyModel]
    deleteManyModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedDeleteManyModel]
  }

  it should "be able to create ClientNamespacedDeleteManyModel with options" in {
    val options = ClientDeleteManyOptions.clientDeleteManyOptions()
    val deleteManyModel = ClientNamespacedWriteModel.deleteMany(namespace, filter, options)
    deleteManyModel shouldBe a[ClientNamespacedDeleteManyModel]
    deleteManyModel shouldBe a[com.mongodb.client.model.bulk.ClientNamespacedDeleteManyModel]
  }
}
