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

import com.mongodb.reactivestreams.client.gridfs.{GridFSBuckets, GridFSBucket => JGridFSBucket}
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.bson.{BsonValue, ObjectId}
import org.mongodb.scala.{ClientSession, Completed, MongoDatabase, Observable, ReadConcern, ReadPreference, SingleObservable, WriteConcern}

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
  def apply(database: MongoDatabase, bucketName: String): GridFSBucket = GridFSBucket(GridFSBuckets.create(database.wrapped, bucketName))
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
  def withReadPreference(readPreference: ReadPreference): GridFSBucket = GridFSBucket(wrapped.withReadPreference(readPreference))

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
   * Opens a AsyncOutputStream that the application can write the contents of the file to.
   *
   * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
   * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
   * document is created in the files collection.
   *
   * @param filename the filename for the stream
   * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
   *         application will write the contents.
   */
  def openUploadStream(filename: String): GridFSUploadStream = GridFSUploadStream(wrapped.openUploadStream(filename))

  /**
   * Opens a AsyncOutputStream that the application can write the contents of the file to.
   *
   * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
   * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
   * document is created in the files collection.
   *
   * @param filename the filename for the stream
   * @param options  the GridFSUploadOptions
   * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
   *         application will write the contents.
   */
  def openUploadStream(filename: String, options: GridFSUploadOptions): GridFSUploadStream =
    GridFSUploadStream(wrapped.openUploadStream(filename, options))

  /**
   * Opens a AsyncOutputStream that the application can write the contents of the file to.
   *
   * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
   * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
   * document is created in the files collection.
   *
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
   *         application will write the contents.
   */
  def openUploadStream(id: BsonValue, filename: String): GridFSUploadStream = GridFSUploadStream(wrapped.openUploadStream(id, filename))

  /**
   * Opens a AsyncOutputStream that the application can write the contents of the file to.
   *
   * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
   * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
   * document is created in the files collection.
   *
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @param options  the GridFSUploadOptions
   * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
   *         application will write the contents.
   */
  def openUploadStream(id: BsonValue, filename: String, options: GridFSUploadOptions): GridFSUploadStream =
    GridFSUploadStream(wrapped.openUploadStream(id, filename, options))

  /**
   * Opens a AsyncOutputStream that the application can write the contents of the file to.
   *
   * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
   * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
   * document is created in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename the filename for the stream
   * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
   *         application will write the contents.
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def openUploadStream(clientSession: ClientSession, filename: String): GridFSUploadStream =
    GridFSUploadStream(wrapped.openUploadStream(clientSession, filename))

  /**
   * Opens a AsyncOutputStream that the application can write the contents of the file to.
   *
   * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
   * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
   * document is created in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename the filename for the stream
   * @param options  the GridFSUploadOptions
   * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
   *         application will write the contents.
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def openUploadStream(clientSession: ClientSession, filename: String, options: GridFSUploadOptions): GridFSUploadStream =
    GridFSUploadStream(wrapped.openUploadStream(clientSession, filename, options))

  /**
   * Opens a AsyncOutputStream that the application can write the contents of the file to.
   *
   * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
   * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
   * document is created in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
   *         application will write the contents.
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def openUploadStream(clientSession: ClientSession, id: BsonValue, filename: String): GridFSUploadStream =
    GridFSUploadStream(wrapped.openUploadStream(clientSession, id, filename))

  /**
   * Opens a AsyncOutputStream that the application can write the contents of the file to.
   *
   * As the application writes the contents to the returned Stream, the contents are uploaded as chunks in the chunks collection. When
   * the application signals it is done writing the contents of the file by calling close on the returned Stream, a files collection
   * document is created in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @param options  the GridFSUploadOptions
   * @return the GridFSUploadStream that provides the ObjectId for the file to be uploaded and the Stream to which the
   *         application will write the contents.
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def openUploadStream(clientSession: ClientSession, id: BsonValue, filename: String, options: GridFSUploadOptions): GridFSUploadStream =
    GridFSUploadStream(wrapped.openUploadStream(clientSession, id, filename, options))

  /**
   * Uploads the contents of the given [[AsyncInputStream]] to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param filename the filename for the stream
   * @param source   the Stream providing the file data
   * @return a Observable returning a single element containing the ObjectId of the uploaded file.
   */
  def uploadFromStream(filename: String, source: AsyncInputStream): Observable[ObjectId] = wrapped.uploadFromStream(filename, source)

  /**
   * Uploads the contents of the given `AsyncInputStream` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param filename the filename for the stream
   * @param source   the Stream providing the file data
   * @param options  the GridFSUploadOptions
   * @return a Observable returning a single element containing the ObjectId of the uploaded file.
   */
  def uploadFromStream(filename: String, source: AsyncInputStream, options: GridFSUploadOptions): Observable[ObjectId] =
    wrapped.uploadFromStream(filename, source, options)

  /**
   * Uploads the contents of the given `AsyncInputStream` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @param source   the Stream providing the file data
   * @return a Observable with a single element indicating when the operation has completed
   */
  def uploadFromStream(id: BsonValue, filename: String, source: AsyncInputStream): SingleObservable[Completed] =
    wrapped.uploadFromStream(id, filename, source)

  /**
   * Uploads the contents of the given `AsyncInputStream` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @param source   the Stream providing the file data
   * @param options  the GridFSUploadOptions
   * @return a Observable with a single element indicating when the operation has completed
   */
  def uploadFromStream(id: BsonValue, filename: String, source: AsyncInputStream, options: GridFSUploadOptions): SingleObservable[Completed] =
    wrapped.uploadFromStream(id, filename, source, options)

  /**
   * Uploads the contents of the given `AsyncInputStream` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename the filename for the stream
   * @param source   the Stream providing the file data
   * @return a Observable returning a single element containing the ObjectId of the uploaded file.
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def uploadFromStream(clientSession: ClientSession, filename: String, source: AsyncInputStream): Observable[ObjectId] =
    wrapped.uploadFromStream(clientSession, filename, source)

  /**
   * Uploads the contents of the given `AsyncInputStream` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename the filename for the stream
   * @param source   the Stream providing the file data
   * @param options  the GridFSUploadOptions
   * @return a Observable returning a single element containing the ObjectId of the uploaded file.
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def uploadFromStream(clientSession: ClientSession, filename: String, source: AsyncInputStream, options: GridFSUploadOptions): Observable[ObjectId] =
    wrapped.uploadFromStream(clientSession, filename, source, options)

  /**
   * Uploads the contents of the given `AsyncInputStream` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @param source   the Stream providing the file data
   * @return a Observable with a single element indicating when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def uploadFromStream(clientSession: ClientSession, id: BsonValue, filename: String, source: AsyncInputStream): SingleObservable[Completed] =
    wrapped.uploadFromStream(clientSession, id, filename, source)

  /**
   * Uploads the contents of the given `AsyncInputStream` to a GridFS bucket.
   *
   * Reads the contents of the user file from the `source` and uploads it as chunks in the chunks collection. After all the
   * chunks have been uploaded, it creates a files collection document for `filename` in the files collection.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id       the custom id value of the file
   * @param filename the filename for the stream
   * @param source   the Stream providing the file data
   * @param options  the GridFSUploadOptions
   * @return a Observable with a single element indicating when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def uploadFromStream(clientSession: ClientSession, id: BsonValue, filename: String, source: AsyncInputStream,
                       options: GridFSUploadOptions): SingleObservable[Completed] =
    wrapped.uploadFromStream(clientSession, id, filename, source, options)

  /**
   * Opens a AsyncInputStream from which the application can read the contents of the stored file specified by `id`.
   *
   * @param id the ObjectId of the file to be put into a stream.
   * @return the stream
   */
  def openDownloadStream(id: ObjectId): GridFSDownloadStream = GridFSDownloadStream(wrapped.openDownloadStream(id))

  /**
   * Opens a AsyncInputStream from which the application can read the contents of the stored file specified by `id`.
   *
   * @param id the custom id value of the file, to be put into a stream.
   * @return the stream
   */
  def openDownloadStream(id: BsonValue): GridFSDownloadStream = GridFSDownloadStream(wrapped.openDownloadStream(id))

  /**
   * Opens a Stream from which the application can read the contents of the latest version of the stored file specified by the
   * `filename`.
   *
   * @param filename the name of the file to be downloaded
   * @return the stream
   */
  def openDownloadStream(filename: String): GridFSDownloadStream = GridFSDownloadStream(wrapped.openDownloadStream(filename))

  /**
   * Opens a Stream from which the application can read the contents of the stored file specified by `filename` and the revision
   * in `options`.
   *
   * @param filename the name of the file to be downloaded
   * @param options  the download options
   * @return the stream
   */
  def openDownloadStream(filename: String, options: GridFSDownloadOptions): GridFSDownloadStream =
    GridFSDownloadStream(wrapped.openDownloadStream(filename, options))

  /**
   * Opens a AsyncInputStream from which the application can read the contents of the stored file specified by `id`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id the ObjectId of the file to be put into a stream.
   * @return the stream
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def openDownloadStream(clientSession: ClientSession, id: ObjectId): GridFSDownloadStream =
    GridFSDownloadStream(wrapped.openDownloadStream(clientSession, id))

  /**
   * Opens a AsyncInputStream from which the application can read the contents of the stored file specified by `id`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id the custom id value of the file, to be put into a stream.
   * @return the stream
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def openDownloadStream(clientSession: ClientSession, id: BsonValue): GridFSDownloadStream =
    GridFSDownloadStream(wrapped.openDownloadStream(clientSession, id))

  /**
   * Opens a Stream from which the application can read the contents of the latest version of the stored file specified by the
   * `filename`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename the name of the file to be downloaded
   * @return the stream
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def openDownloadStream(clientSession: ClientSession, filename: String): GridFSDownloadStream =
    GridFSDownloadStream(wrapped.openDownloadStream(clientSession, filename))

  /**
   * Opens a Stream from which the application can read the contents of the stored file specified by `filename` and the revision
   * in `options`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename the name of the file to be downloaded
   * @param options  the download options
   * @return the stream
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def openDownloadStream(clientSession: ClientSession, filename: String, options: GridFSDownloadOptions): GridFSDownloadStream =
    GridFSDownloadStream(wrapped.openDownloadStream(clientSession, filename, options))

  /**
   * Downloads the contents of the stored file specified by `id` and writes the contents to the `destination`
   * AsyncOutputStream.
   *
   * @param id          the ObjectId of the file to be written to the destination stream
   * @param destination the destination stream
   * @return a Observable with a single element indicating the file has been downloaded
   */
  def downloadToStream(id: ObjectId, destination: AsyncOutputStream): SingleObservable[Long] =
    wrapped.downloadToStream(id, destination)

  /**
   * Downloads the contents of the stored file specified by `id` and writes the contents to the `destination`
   * AsyncOutputStream.
   *
   * @param id          the custom id of the file, to be written to the destination stream
   * @param destination the destination stream
   * @return a Observable with a single element indicating the file has been downloaded
   */
  def downloadToStream(id: BsonValue, destination: AsyncOutputStream): SingleObservable[Long] =
    wrapped.downloadToStream(id, destination)

  /**
   * Downloads the contents of the latest version of the stored file specified by `filename` and writes the contents to
   * the `destination` Stream.
   *
   * @param filename    the name of the file to be downloaded
   * @param destination the destination stream
   * @return a Observable with a single element indicating the file has been downloaded
   */
  def downloadToStream(filename: String, destination: AsyncOutputStream): SingleObservable[Long] =
    wrapped.downloadToStream(filename, destination)

  /**
   * Downloads the contents of the stored file specified by `filename` and by the revision in `options` and writes the
   * contents to the `destination` Stream.
   *
   * @param filename    the name of the file to be downloaded
   * @param destination the destination stream
   * @param options     the download options
   * @return a Observable with a single element indicating the file has been downloaded
   */
  def downloadToStream(filename: String, destination: AsyncOutputStream, options: GridFSDownloadOptions): SingleObservable[Long] =
    wrapped.downloadToStream(filename, destination, options)

  /**
   * Downloads the contents of the stored file specified by `id` and writes the contents to the `destination`
   * AsyncOutputStream.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id          the ObjectId of the file to be written to the destination stream
   * @param destination the destination stream
   * @return a Observable with a single element indicating the file has been downloaded
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def downloadToStream(clientSession: ClientSession, id: ObjectId, destination: AsyncOutputStream): SingleObservable[Long] =
    wrapped.downloadToStream(clientSession, id, destination)

  /**
   * Downloads the contents of the stored file specified by `id` and writes the contents to the `destination`
   * AsyncOutputStream.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id          the custom id of the file, to be written to the destination stream
   * @param destination the destination stream
   * @return a Observable with a single element indicating the file has been downloaded
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def downloadToStream(clientSession: ClientSession, id: BsonValue, destination: AsyncOutputStream): SingleObservable[Long] =
    wrapped.downloadToStream(clientSession, id, destination)

  /**
   * Downloads the contents of the latest version of the stored file specified by `filename` and writes the contents to
   * the `destination` Stream.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename    the name of the file to be downloaded
   * @param destination the destination stream
   * @return a Observable with a single element indicating the file has been downloaded
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def downloadToStream(clientSession: ClientSession, filename: String, destination: AsyncOutputStream): SingleObservable[Long] =
    wrapped.downloadToStream(clientSession, filename, destination)

  /**
   * Downloads the contents of the stored file specified by `filename` and by the revision in `options` and writes the
   * contents to the `destination` Stream.
   *
   * @param clientSession the client session with which to associate this operation
   * @param filename    the name of the file to be downloaded
   * @param destination the destination stream
   * @param options     the download options
   * @return a Observable with a single element indicating the file has been downloaded
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def downloadToStream(clientSession: ClientSession, filename: String, destination: AsyncOutputStream, options: GridFSDownloadOptions): SingleObservable[Long] =
    wrapped.downloadToStream(clientSession, filename, destination, options)

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
   * @return a Observable with a single element indicating when the operation has completed
   */
  def delete(id: ObjectId): SingleObservable[Completed] = wrapped.delete(id)

  /**
   * Given a `id`, delete this stored file's files collection document and associated chunks from a GridFS bucket.
   *
   * @param id       the ObjectId of the file to be deleted
   * @return a Observable with a single element indicating when the operation has completed
   */
  def delete(id: BsonValue): SingleObservable[Completed] = wrapped.delete(id)

  /**
   * Given a `id`, delete this stored file's files collection document and associated chunks from a GridFS bucket.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id       the ObjectId of the file to be deleted
   * @return a Observable with a single element indicating when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def delete(clientSession: ClientSession, id: ObjectId): SingleObservable[Completed] =
    wrapped.delete(clientSession, id)

  /**
   * Given a `id`, delete this stored file's files collection document and associated chunks from a GridFS bucket.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id       the ObjectId of the file to be deleted
   * @return a Observable with a single element indicating when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def delete(clientSession: ClientSession, id: BsonValue): SingleObservable[Completed] =
    wrapped.delete(clientSession, id)

  /**
   * Renames the stored file with the specified `id`.
   *
   * @param id          the id of the file in the files collection to rename
   * @param newFilename the new filename for the file
   * @return a Observable with a single element indicating when the operation has completed
   */
  def rename(id: ObjectId, newFilename: String): SingleObservable[Completed] =
    wrapped.rename(id, newFilename)

  /**
   * Renames the stored file with the specified `id`.
   *
   * @param id          the id of the file in the files collection to rename
   * @param newFilename the new filename for the file
   * @return a Observable with a single element indicating when the operation has completed
   */
  def rename(id: BsonValue, newFilename: String): SingleObservable[Completed] =
    wrapped.rename(id, newFilename)

  /**
   * Renames the stored file with the specified `id`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id          the id of the file in the files collection to rename
   * @param newFilename the new filename for the file
   * @return a Observable with a single element indicating when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def rename(clientSession: ClientSession, id: ObjectId, newFilename: String): SingleObservable[Completed] =
    wrapped.rename(clientSession, id, newFilename)

  /**
   * Renames the stored file with the specified `id`.
   *
   * @param clientSession the client session with which to associate this operation
   * @param id          the id of the file in the files collection to rename
   * @param newFilename the new filename for the file
   * @return a Observable with a single element indicating when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def rename(clientSession: ClientSession, id: BsonValue, newFilename: String): SingleObservable[Completed] =
    wrapped.rename(clientSession, id, newFilename)

  /**
   * Drops the data associated with this bucket from the database.
   *
   * @return a Observable with a single element indicating when the operation has completed
   */
  def drop(): SingleObservable[Completed] = wrapped.drop()

  /**
   * Drops the data associated with this bucket from the database.
   *
   * @param clientSession the client session with which to associate this operation
   * @return a Observable with a single element indicating when the operation has completed
   * @since 2.2
   * @note Requires MongoDB 3.6 or greater
   */
  def drop(clientSession: ClientSession): SingleObservable[Completed] = wrapped.drop(clientSession)
}
// scalastyle:on number.of.methods
