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

package org.mongodb.scala.gridfs

import java.nio.ByteBuffer

import com.mongodb.reactivestreams.client.gridfs.{ GridFSBucket => JGridFSBucket, GridFSBuckets }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.bson.{ BsonObjectId, BsonValue, ObjectId }
import org.mongodb.scala.{
  ClientSession,
  MongoDatabase,
  Observable,
  ReadConcern,
  ReadPreference,
  SingleObservable,
  WriteConcern
}

/**
 * A factory for GridFSBucket instances.
 *
 * @since 1.2
 */
object GridFSBucket {

  /**
   * Create a new GridFS bucket with the default `'fs'` bucket name
   *
   * @param database the database instance to use with GridFS
   * @return the GridFSBucket
   */
  def apply(database: MongoDatabase): GridFSBucket = GridFSBucket(GridFSBuckets.create(database.wrapped))

  /**
   * Create a new GridFS bucket with a custom bucket name
   *
   * @param database   the database instance to use with GridFS
   * @param bucketName the custom bucket name to use
   * @return the GridFSBucket
   */
  def apply(database: MongoDatabase, bucketName: String): GridFSBucket =
    GridFSBucket(GridFSBuckets.create(database.wrapped, bucketName))
}

// scalastyle:off number.of.methods
/**
 * Represents a GridFS Bucket
 *
 * @since 1.2
 */
case class GridFSBucket(private val wrapped: JGridFSBucket) {

  /**
   * The bucket name.
   *
   * @return the bucket name
   */
  lazy val bucketName: String = wrapped.getBucketName

  /**
   * Sets the chunk size in bytes. Defaults to 255.
   *
   * @return the chunk size in bytes.
   */
  lazy val chunkSizeBytes: Int = wrapped.getChunkSizeBytes

  /**
   * Get the write concern for the GridFSBucket.
   *
   * @return the WriteConcern
   */
  lazy val writeConcern: WriteConcern = wrapped.getWriteConcern

  /**
   * Get the read preference for the GridFSBucket.
   *
   * @return the ReadPreference
   */
  lazy val readPreference: ReadPreference = wrapped.getReadPreference

  /**
   * Get the read concern for the GridFSBucket.
   *
   * @return the ReadConcern
   * @note Requires MongoDB 3.2 or greater
   * @see [[http://docs.mongodb.org/manual/reference/readConcern Read Concern]]
   */
  lazy val readConcern: ReadConcern = wrapped.getReadConcern

  /**
   * Create a new GridFSBucket instance with a new chunk size in bytes.
   *
   * @param chunkSizeBytes the new chunk size in bytes.
   * @return a new GridFSBucket instance with the different chunk size in bytes
   */
  def withChunkSizeBytes(chunkSizeBytes: Int): GridFSBucket = GridFSBucket(wrapped.withChunkSizeBytes(chunkSizeBytes))

  /**
   * Create a new GridFSBucket instance with a different read preference.
   *
   * @param readPreference the new ReadPreference for the database
   * @return a new GridFSBucket instance with the different readPreference
   */
  def withReadPreference(readPreference: ReadPreference): GridFSBucket =
    GridFSBucket(wrapped.withReadPreference(readPreference))

  /**
   * Create a new GridFSBucket instance with a different write concern.
   *
   * @param writeConcern the new WriteConcern for the database
   * @return a new GridFSBucket instance with the different writeConcern
   */
  def withWriteConcern(writeConcern: WriteConcern): GridFSBucket = GridFSBucket(wrapped.withWriteConcern(writeConcern))

  /**
   * Create a new MongoDatabase instance with a different read concern.
   *
   * @param readConcern the new ReadConcern for the database
   * @return a new GridFSBucket instance with the different ReadConcern
   * @note Requires MongoDB 3.2 or greater
   * @see [[http://docs.mongodb.org/manual/reference/readConcern Read Concern]]
   */
  def withReadConcern(readConcern: ReadConcern): GridFSBucket = GridFSBucket(wrapped.withReadConcern(readConcern))

  /**
   * Uploads the contents of the given `Observable` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   *
   * @param filename the filename for the stream
   * @param source   the Publisher providing the file data
   * @return an Observable with a single element, the ObjectId of the uploaded file.
   * @since 2.8
   */
  def uploadFromObservable(filename: String, source: Observable[ByteBuffer]): GridFSUploadObservable[ObjectId] =
    GridFSUploadObservable(wrapped.uploadFromPublisher(filename, source))

  /**
   * Uploads the contents of the given `Observable` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   *
   * @param filename the filename for the stream
   * @param source   the Publisher providing the file data
   * @param options  the GridFSUploadOptions
   * @return an Observable with a single element, the ObjectId of the uploaded file.
   * @since 2.8
   */
  def uploadFromObservable(
      filename: String,
      source: Observable[ByteBuffer],
      options: GridFSUploadOptions
  ): GridFSUploadObservable[ObjectId] =
    GridFSUploadObservable(wrapped.uploadFromPublisher(filename, source, options))

  /**
   * Uploads the contents of the given `Observable` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   *
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @param source   the Publisher providing the file data
   * @return an Observable with a single element, representing when the successful upload of the source.
   * @since 2.8
   */
  def uploadFromObservable(
      id: BsonValue,
      filename: String,
      source: Observable[ByteBuffer]
  ): GridFSUploadObservable[Void] =
    GridFSUploadObservable(wrapped.uploadFromPublisher(id, filename, source))

  /**
   * Uploads the contents of the given `Observable` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   *
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @param source   the Publisher providing the file data
   * @param options  the GridFSUploadOptions
   * @return an Observable with a single element, representing when the successful upload of the source.
   * @since 2.8
   */
  def uploadFromObservable(
      id: BsonValue,
      filename: String,
      source: Observable[ByteBuffer],
      options: GridFSUploadOptions
  ): GridFSUploadObservable[Void] =
    GridFSUploadObservable(wrapped.uploadFromPublisher(id, filename, source, options))

  /**
   * Uploads the contents of the given `Observable` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename      the filename for the stream
   * @param source        the Publisher providing the file data
   * @return an Observable with a single element, the ObjectId of the uploaded file.
   * @note Requires MongoDB 3.6 or greater
   * @since 2.8
   */
  def uploadFromObservable(
      clientSession: ClientSession,
      filename: String,
      source: Observable[ByteBuffer]
  ): GridFSUploadObservable[ObjectId] =
    GridFSUploadObservable(wrapped.uploadFromPublisher(clientSession, filename, source))

  /**
   * Uploads the contents of the given `Observable` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename      the filename for the stream
   * @param source        the Publisher providing the file data
   * @param options       the GridFSUploadOptions
   * @return an Observable with a single element, the ObjectId of the uploaded file.
   * @note Requires MongoDB 3.6 or greater
   * @since 2.8
   */
  def uploadFromObservable(
      clientSession: ClientSession,
      filename: String,
      source: Observable[ByteBuffer],
      options: GridFSUploadOptions
  ): GridFSUploadObservable[ObjectId] =
    GridFSUploadObservable(wrapped.uploadFromPublisher(clientSession, filename, source, options))

  /**
   * Uploads the contents of the given `Observable` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   *
   * @param clientSession the client session with which to associate this operation
   * @param id            the custom id value of the file
   * @param filename      the filename for the stream
   * @param source        the Publisher providing the file data
   * @return an Observable with a single element, representing when the successful upload of the source.
   * @note Requires MongoDB 3.6 or greater
   * @since 2.8
   */
  def uploadFromObservable(
      clientSession: ClientSession,
      id: BsonValue,
      filename: String,
      source: Observable[ByteBuffer]
  ): GridFSUploadObservable[Void] =
    GridFSUploadObservable(wrapped.uploadFromPublisher(clientSession, id, filename, source))

  /**
   * Uploads the contents of the given `Observable` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id            the custom id value of the file
   * @param filename      the filename for the stream
   * @param source        the Publisher providing the file data
   * @param options       the GridFSUploadOptions
   * @return an Observable with a single element, representing when the successful upload of the source.
   * @note Requires MongoDB 3.6 or greater
   * @since 2.8
   */
  def uploadFromObservable(
      clientSession: ClientSession,
      id: BsonValue,
      filename: String,
      source: Observable[ByteBuffer],
      options: GridFSUploadOptions
  ): GridFSUploadObservable[Void] =
    GridFSUploadObservable(wrapped.uploadFromPublisher(clientSession, id, filename, source, options))

  /**
   * Downloads the contents of the stored file specified by `id` into the `Publisher`.
   *
   * @param id the ObjectId of the file to be written to the destination stream
   * @return an Observable with a single element, representing the amount of data written
   * @since 2.8
   */
  def downloadToObservable(id: ObjectId): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.downloadToPublisher(id))

  /**
   * Downloads the contents of the stored file specified by `id` into the `Publisher`.
   *
   * @param id the custom id of the file, to be written to the destination stream
   * @return an Observable with a single element, representing the amount of data written
   * @since 2.8
   */
  def downloadToObservable(id: BsonValue): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.downloadToPublisher(id))

  /**
   * Downloads the contents of the stored file specified by `filename` into the `Publisher`.
   *
   * @param filename the name of the file to be downloaded
   * @return an Observable with a single element, representing the amount of data written
   * @since 2.8
   */
  def downloadToObservable(filename: String): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.downloadToPublisher(filename))

  /**
   * Downloads the contents of the stored file specified by `filename` and by the revision in `options` into the
   * `Publisher`.
   *
   * @param filename the name of the file to be downloaded
   * @param options  the download options
   * @return an Observable with a single element, representing the amount of data written
   * @since 2.8
   */
  def downloadToObservable(filename: String, options: GridFSDownloadOptions): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.downloadToPublisher(filename, options))

  /**
   * Downloads the contents of the stored file specified by `id` into the `Publisher`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id            the ObjectId of the file to be written to the destination stream
   * @return an Observable with a single element, representing the amount of data written
   * @note Requires MongoDB 3.6 or greater
   * @since 2.8
   */
  def downloadToObservable(clientSession: ClientSession, id: ObjectId): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.downloadToPublisher(clientSession, id))

  /**
   * Downloads the contents of the stored file specified by `id` into the `Publisher`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id            the custom id of the file, to be written to the destination stream
   * @return an Observable with a single element, representing the amount of data written
   * @note Requires MongoDB 3.6 or greater
   * @since 2.8
   */
  def downloadToObservable(clientSession: ClientSession, id: BsonValue): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.downloadToPublisher(clientSession, id))

  /**
   * Downloads the contents of the latest version of the stored file specified by `filename` into the `Publisher`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename      the name of the file to be downloaded
   * @return an Observable with a single element, representing the amount of data written
   * @note Requires MongoDB 3.6 or greater
   * @since 2.8
   */
  def downloadToObservable(clientSession: ClientSession, filename: String): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.downloadToPublisher(clientSession, filename))

  /**
   * Downloads the contents of the stored file specified by `filename` and by the revision in `options` into the
   * `Publisher`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename      the name of the file to be downloaded
   * @param options       the download options
   * @return an Observable with a single element, representing the amount of data written
   * @note Requires MongoDB 3.6 or greater
   * @since 2.8
   */
  def downloadToObservable(
      clientSession: ClientSession,
      filename: String,
      options: GridFSDownloadOptions
  ): GridFSDownloadObservable =
    GridFSDownloadObservable(wrapped.downloadToPublisher(clientSession, filename, options))

  /**
   * Finds all documents in the files collection.
   *
   * @return the GridFS find iterable interface
   * @see [[http://docs.mongodb.org/manual/tutorial/query-documents/ Find]]
   */
  def find(): GridFSFindObservable = GridFSFindObservable(wrapped.find())

  /**
   * Finds all documents in the collection that match the filter.
   *
   * Below is an example of filtering against the filename and some nested metadata that can also be stored along with the file data:
   *
   * `
   * Filters.and(Filters.eq("filename", "mongodb.png"), Filters.eq("metadata.contentType", "image/png"));
   * `
   *
   * @param filter the query filter
   * @return the GridFS find iterable interface
   * @see com.mongodb.client.model.Filters
   */
  def find(filter: Bson): GridFSFindObservable = GridFSFindObservable(wrapped.find(filter))

  /**
   * Finds all documents in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @return the GridFS find iterable interface
   * @see [[http://docs.mongodb.org/manual/tutorial/query-documents/ Find]]
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def find(clientSession: ClientSession): GridFSFindObservable = GridFSFindObservable(wrapped.find(clientSession))

  /**
   * Finds all documents in the collection that match the filter.
   *
   * Below is an example of filtering against the filename and some nested metadata that can also be stored along with the file data:
   *
   * `
   * Filters.and(Filters.eq("filename", "mongodb.png"), Filters.eq("metadata.contentType", "image/png"));
   * `
   *
   * @param clientSession the client session with which to associate this operation
   * @param filter the query filter
   * @return the GridFS find iterable interface
   * @see com.mongodb.client.model.Filters
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def find(clientSession: ClientSession, filter: Bson): GridFSFindObservable =
    GridFSFindObservable(wrapped.find(clientSession, filter))

  /**
   * Given a `id`, delete this stored file's files collection document and associated chunks from a GridFS bucket.
   *
   * @param id       the ObjectId of the file to be deleted
   * @return an empty Observable that indicates when the operation has completed
   */
  def delete(id: ObjectId): SingleObservable[Void] = wrapped.delete(id)

  /**
   * Given a `id`, delete this stored file's files collection document and associated chunks from a GridFS bucket.
   *
   * @param id       the ObjectId of the file to be deleted
   * @return an empty Observable that indicates when the operation has completed
   */
  def delete(id: BsonValue): SingleObservable[Void] = wrapped.delete(id)

  /**
   * Given a `id`, delete this stored file's files collection document and associated chunks from a GridFS bucket.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id       the ObjectId of the file to be deleted
   * @return an empty Observable that indicates when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def delete(clientSession: ClientSession, id: ObjectId): SingleObservable[Void] =
    wrapped.delete(clientSession, id)

  /**
   * Given a `id`, delete this stored file's files collection document and associated chunks from a GridFS bucket.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id       the ObjectId of the file to be deleted
   * @return an empty Observable that indicates when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def delete(clientSession: ClientSession, id: BsonValue): SingleObservable[Void] =
    wrapped.delete(clientSession, id)

  /**
   * Renames the stored file with the specified `id`.
   *
   * @param id          the id of the file in the files collection to rename
   * @param newFilename the new filename for the file
   * @return an empty Observable that indicates when the operation has completed
   */
  def rename(id: ObjectId, newFilename: String): SingleObservable[Void] =
    wrapped.rename(id, newFilename)

  /**
   * Renames the stored file with the specified `id`.
   *
   * @param id          the id of the file in the files collection to rename
   * @param newFilename the new filename for the file
   * @return an empty Observable that indicates when the operation has completed
   */
  def rename(id: BsonValue, newFilename: String): SingleObservable[Void] =
    wrapped.rename(id, newFilename)

  /**
   * Renames the stored file with the specified `id`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id          the id of the file in the files collection to rename
   * @param newFilename the new filename for the file
   * @return an empty Observable that indicates when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def rename(clientSession: ClientSession, id: ObjectId, newFilename: String): SingleObservable[Void] =
    wrapped.rename(clientSession, id, newFilename)

  /**
   * Renames the stored file with the specified `id`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id          the id of the file in the files collection to rename
   * @param newFilename the new filename for the file
   * @return an empty Observable that indicates when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def rename(clientSession: ClientSession, id: BsonValue, newFilename: String): SingleObservable[Void] =
    wrapped.rename(clientSession, id, newFilename)

  /**
   * Drops the data associated with this bucket from the database.
   *
   * @return an empty Observable that indicates when the operation has completed
   */
  def drop(): SingleObservable[Void] = wrapped.drop()

  /**
   * Drops the data associated with this bucket from the database.
   *
   * @param clientSession the client session with which to associate this operation
   * @return an empty Observable that indicates when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def drop(clientSession: ClientSession): SingleObservable[Void] = wrapped.drop(clientSession)
}
// scalastyle:on number.of.methods
