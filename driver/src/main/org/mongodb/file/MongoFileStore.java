package org.mongodb.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.types.ObjectId;
import org.mongodb.CommandResult;
import org.mongodb.Document;
import org.mongodb.Index;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCollectionOptions;
import org.mongodb.MongoCursor;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoException;
import org.mongodb.MongoView;
import org.mongodb.OrderBy;
import org.mongodb.WriteConcern;
import org.mongodb.WriteResult;
import org.mongodb.diagnostics.Loggers;
import org.mongodb.diagnostics.logging.Logger;
import org.mongodb.file.url.MongoFileUrl;

public class MongoFileStore {

    private static final Logger LOGGER = Loggers.getLogger("file");

    private final MongoCollection<Document> filesCollection;
    private final MongoCollection<Document> chunksCollection;

    private MongoFileStoreConfig config;

    /**
     * CTOR
     * 
     * @param database
     *            the MongoDB database to look for the collections in
     * @param config
     *            the configuration for this file store
     */
    public MongoFileStore(final MongoDatabase database, final MongoFileStoreConfig config) {

        this.config = config;

        // FILES
        MongoCollectionOptions fileOptions = (MongoCollectionOptions) MongoCollectionOptions.builder()//
                .writeConcern(config.getWriteConcern())//
                .readPreference(config.getReadPreference())//
                .build();

        filesCollection = database.getCollection(config.getBucket() + ".files", fileOptions);

        // CHUNKS
        MongoCollectionOptions chunksOptions = (MongoCollectionOptions) MongoCollectionOptions.builder()//
                .writeConcern(config.getWriteConcern())//
                .readPreference(config.getReadPreference())//
                .build();
        chunksCollection = database.getCollection(config.getBucket() + ".chunks", chunksOptions);

        // make sure the expiration index is present
        // files go first, within a minute according to MongoDB
        // then the chunks follow 1 minute after the file objects
        checkForExpireAtIndex(filesCollection, 0);
        checkForExpireAtIndex(chunksCollection, 60);

        // ensure standard indexes as long as collections are small
        try {
            if (getCollectionStats(filesCollection) < 1000) {
                createIdIndexes(filesCollection, chunksCollection);
            }
        } catch (MongoException e) {
            LOGGER.info(String.format("Unable to ensure indices on GridFS collections in database %s", //
                    filesCollection.getDatabase().getName()));
        }
    }

    private int getCollectionStats(final MongoCollection<Document> coll) {
        // { collStats: "collection" , scale : 1024 }
        CommandResult result = coll.getDatabase().executeCommand(new Document("collStats", coll.getName()).append("scale", 1024));
        return result.isOk() ? result.getResponse().getInteger("size").intValue() : 0;
    }

    private void checkForExpireAtIndex(final MongoCollection<Document> coll, final int secondsDelay) {

        // check for existing
        for (Document document : coll.tools().getIndexes()) {
            if ("ttl".equals(document.get("name"))) {
                return;
            }
        }

        // build one
        Index idx = Index.builder()//
                .addKey("expireAt", OrderBy.ASC)//
                .expireAfterSeconds(secondsDelay)//
                .name("ttl")//
                .sparse() //
                .background(true)//
                .build();

        coll.tools().createIndexes(java.util.Collections.singletonList(idx));

    }

    private void createIdIndexes(final MongoCollection<Document> fileColl, final MongoCollection<Document> chunksColl) {
        Index filesIdx = Index.builder()//
                .name("filename")//
                .addKey("filename", OrderBy.ASC)//
                .addKey("uploadDate", OrderBy.ASC).background(true)//
                .build();

        fileColl.tools().createIndexes(java.util.Collections.singletonList(filesIdx));

        Index chunksIdx = Index.builder()//
                .name("files_id")//
                .addKey("files_id", OrderBy.ASC)//
                .addKey("n", OrderBy.ASC).unique()//
                .background(true)//
                .build();

        chunksColl.tools().createIndexes(java.util.Collections.singletonList(chunksIdx));

    }

    //
    // public
    // ///////////////

    public int getChunkSize() {

        return config.getChunkSize();
    }

    /**
     * Run a test command to the mongoDB to test connectivity and the server is running
     * 
     * @return true if a connection could be made
     * 
     * @throws MongoException
     */
    public boolean validateConnection() {

        try {
            // String command = String.format(
            // "{ touch: \"%s\", data: false, index: true }",
            // config.getBucket() + ".files");

            Document doc = new Document() //
                    .append("touch", config.getBucket() + ".files") //
                    .append("data", Boolean.FALSE) //
                    .append("index", Boolean.TRUE);

            CommandResult commandResult = filesCollection.getDatabase().executeCommand(doc);
            if (!commandResult.isOk()) {
                throw new MongoException(commandResult.getErrorMessage());
            }

            return true;
        } catch (Exception e) {
            throw new MongoException("Unable to run command on server", e);
        }
    }

    //
    // writing
    // //////////////////

    /**
     * Create a new file entry in the datastore, then a MongoFile object to start writing to it.
     * 
     * NOTE : the system will determine if compression is needed
     * 
     * @param filename
     *            the name of the new file
     * @param mediaType
     *            the media type of the data
     * 
     * @return a writer to write datq to for this file
     * 
     * @throws IOException
     *             if an error occurs during reading and/or writing
     * @throws IllegalArgumentException
     *             if required parameters are null
     */
    public MongoFileWriter createNew(final String filename, final String mediaType) throws IOException {

        return createNew(filename, mediaType, null, config.isEnableCompression());
    }

    /**
     * Create a new file entry in the datastore, then a MongoFile object to start writing to it.
     * 
     * NOTE : if compress = false and the media type is compressible, the file will not be stored compressed in the store
     * 
     * @param filename
     *            the name of the new file
     * @param mediaType
     *            the media type of the data
     * @param expiresAt
     *            the date when the file should be expired
     * @param compress
     *            should use compression if the mime type allows ( zip files will not be compressed even compress = true )
     * 
     * @return a writer to write datq to for this file
     * 
     * @throws IllegalStateException
     *             if compression is disabled in the configuration but ask for on the command line
     * @throws IOException
     *             if an error occurs during reading and/or writing
     * @throws IllegalArgumentException
     *             if required parameters are null
     * 
     */
    public MongoFileWriter createNew(final String filename, final String mediaType, final Date expiresAt, final boolean compress)
            throws IOException {

        if (filename == null) {
            throw new IllegalArgumentException("filename cannot be null");
        }
        if (mediaType == null) {
            throw new IllegalArgumentException("mediaType cannot be null");
        }

        if (compress && !config.isEnableCompression()) {
            throw new IllegalStateException("This data store has compression disabled");
        }

        // send wrapper object
        MongoFileUrl mongoFileUrl = MongoFileUrl//
                .construct(new ObjectId(), filename, mediaType, null, compress);

        MongoFile mongoFile = new MongoFile(this, mongoFileUrl, config.getChunkSize(), mongoFileUrl.isStoredCompressed());
        if (expiresAt != null) {
            mongoFile.setExpiresAt(expiresAt);
        }

        return new MongoFileWriter(mongoFileUrl, mongoFile, chunksCollection);
    }

    /**
     * Upload a file to the datastore from the filesystem
     * 
     * @param file
     *            - the file object to get the data from
     * @param mediaType
     *            the media type of the data
     * 
     * @return the MongoFile object created for this file object
     * 
     * @throws IllegalStateException
     *             if compression is disabled in the configuration but ask for on the command line
     * @throws IOException
     *             if an error occurs during reading and/or writing
     * @throws IllegalArgumentException
     *             if required parameters are null
     * @throws FileNotFoundException
     *             if the file does not exist or cannot be read
     */
    public MongoFile upload(final File file, final String mediaType) throws IOException {

        FileInputStream inputStream = new FileInputStream(file);
        try {
            return upload(file.toPath().toString(), mediaType, null, true, inputStream);
        } finally {
            inputStream.close();
        }
    }

    /**
     * Upload a file to the datastore from the filesystem
     * 
     * @param file
     *            - the file object to get the data from
     * @param mediaType
     *            the media type of the data
     * @param expiresAt
     *            the date in the future that the file should expire.
     * @param compress
     *            allow compression to be used if applicable
     * 
     * @return the MongoFile object created for this file object
     * 
     * @throws IllegalStateException
     *             if compression is disabled in the configuration but ask for on the command line
     * @throws IOException
     *             if an error occurs during reading and/or writing
     * @throws IllegalArgumentException
     *             if required parameters are null
     * @throws FileNotFoundException
     *             if the file does not exist or cannot be read
     */
    public MongoFile upload(final File file, final String mediaType, final boolean compress, final Date expiresAt)
            throws IOException {

        if (file == null) {
            throw new IllegalArgumentException("passed in file cannot be null");
        }

        if (!file.exists()) {
            throw new FileNotFoundException("File does not exist or cannot be read by this library");
        }

        FileInputStream inputStream = new FileInputStream(file);
        try {
            return upload(file.toPath().toString(), mediaType, expiresAt, compress, inputStream);
        } finally {
            inputStream.close();
        }
    }

    /**
     * Upload a file to the datastore from any InputStream
     * 
     * @param filename
     *            the name of the file to use
     * @param mediaType
     *            the media type of the data
     * @param inputStream
     *            the stream object to read the data from
     * 
     * @return the MongoFile object created for this file object
     * 
     * @throws IllegalStateException
     *             if compression is disabled in the configuration but ask for on the command line
     * @throws IOException
     *             if an error occurs during reading and/or writing
     * @throws IllegalArgumentException
     *             if required parameters are null
     */
    public MongoFile upload(final String filename, final String mediaType, final InputStream inputStream) throws IOException {

        return upload(filename, mediaType, null, true, inputStream);

    }

    /**
     * Upload a file to the datastore from any InputStream
     * 
     * @param filename
     *            the name of the file to use
     * @param mediaType
     *            the media type of the data
     * @param expiresAt
     *            the date in the future that the file should expire.
     * @param compress
     *            allow compression to be used if applicable
     * @param inputStream
     *            the stream object to read the data from
     * 
     * @return the MongoFile object created for this file object
     * 
     * @throws IllegalStateException
     *             if compression is disabled in the configuration but ask for on the command line
     * @throws IOException
     *             if an error occurs during reading and/or writing
     * @throws IllegalArgumentException
     *             if required parameters are null
     * 
     */
    public MongoFile upload(final String filename, final String mediaType, final Date expiresAt, final boolean compress,
            final InputStream inputStream) throws IOException {

        return createNew(filename, mediaType, expiresAt, compress).write(inputStream);
    }

    //
    // read
    // ////////////////////

    /**
     * Returns a reader for the passed in URL
     * 
     * @param url
     * 
     * @return a reader object
     * 
     * @throws MongoException
     * @throws IllegalArgumentException
     *             if required parameters are null
     */
    public MongoFile findOne(final URL url) {

        if (url == null) {
            throw new IllegalArgumentException("url cannot be null");
        }
        return findOne(MongoFileUrl.construct(url));
    }

    /**
     * Returns a MongoFile for the passed in file url
     * 
     * @param url
     * 
     * @return a reader object
     * 
     * @throws MongoException
     * @throws IllegalArgumentException
     *             if required parameters are null
     */
    public MongoFile findOne(final MongoFileUrl url) {

        if (url == null) {
            throw new IllegalArgumentException("url cannot be null");
        }
        MongoCursor<Document> cursor = filesCollection.find(//
                new Document().append(MongoFileConstants._id.toString(), url.getMongoFileId())).get();

        if (!cursor.hasNext()) {
            return null;
        }

        Document file = deletedFileCheck(cursor.next());
        return file == null ? null : new MongoFile(this, file);
    }

    /**
     * Returns true is the file exists in the data store
     * 
     * @param url
     * 
     * @return true if the file exists in the datastore
     * 
     * @throws MongoException
     */
    public boolean exists(final MongoFileUrl url) {

        if (url == null) {
            throw new IllegalArgumentException("mongoFile cannot be null");
        }

        return findOne(url) != null;
    }

    /**
     * REturns true if the file can be accessed from the database
     * 
     * @param id
     * @return
     */
    public boolean exists(final ObjectId id) {
        return null != findOne(id);
    }

    /**
     * finds one file matching the given id. Equivalent to findOne(id)
     * 
     * @param id
     * @return the MongoFile object
     * @throws MongoException
     */
    public MongoFile findOne(final ObjectId id) {

        Document one = this.getFilesCollection().find(new Document("_id", id)).getOne();
        if (one == null) {
            return null;
        }
        one = deletedFileCheck(one);
        if (one == null) {
            return null;
        }
        return new MongoFile(this, one);
    }

    /**
     * finds a list of files matching the given filename
     * 
     * @param filename
     * @return the MongoFileCursor object
     * @throws MongoException
     */
    public MongoFileCursor find(final String filename) {

        return find(filename, null);
    }

    /**
     * finds a list of files matching the given filename
     * 
     * @param filename
     * @param sort
     * @return the MongoFileCursor object
     * @throws MongoException
     */
    public MongoFileCursor find(final String filename, final Document sort) {

        return find(new Document(MongoFileConstants.filename.toString(), filename), sort);
    }

    /**
     * finds a list of files matching the given query
     * 
     * @param query
     * @return the MongoFileCursor object
     * @throws MongoException
     */
    public MongoFileCursor find(final Document query) {

        return find(query, null);
    }

    /**
     * finds a list of files matching the given query
     * 
     * @param query
     * @param sort
     * @return the MongoFileCursor object
     * @throws MongoException
     */
    public MongoFileCursor find(final Document query, final Document sort) {

        MongoView<Document> c = this.getFilesCollection().find(query);
        if (sort != null) {
            c.sort(sort);
        }

        MongoCursor<Document> cursor = c.get();
        return new MongoFileCursor(this, cursor);
    }

    //
    // remove methods
    // ////////////////////

    /**
     * Give a file an expiration date so I can be removed and resources its recovered.
     * 
     * Use the TimeMachine DSL to easily create expiration dates.
     * 
     * This uses MongoDB's TTL indexes feature to allow a server background thread to remove the file. According to their
     * documentation, this may not happen immediately at the time the file is set to expire.
     * 
     * 
     * NOTE: The MongoFileStore has remove methods which perform immediate removal of the file in the MongoFileStore.
     * 
     * @param file
     *            the MongoFile to fix
     * @param when
     *            - the date to expire the file by
     * 
     * @throws MalformedURLException
     */
    public void expireFile(final MongoFile file, final Date when) throws MalformedURLException {

        MongoFileUrl url = file.getURL();

        Document filesQuery = new Document("_id", url.getMongoFileId());
        Document chunksQuery = new Document("files_id", url.getMongoFileId());

        setExpiresAt(filesQuery, chunksQuery, when, false);
    }

    /**
     * Remove a file from the database identified by the given MongoFile
     * 
     * NOTE: this is not asynchronous
     * 
     * @param mongoFile
     * @throws IllegalArgumentException
     * @throws MongoException
     * @throws IOException
     */
    public void remove(final MongoFile mongoFile) throws IOException {

        remove(mongoFile, false);
    }

    /**
     * Remove a file from the database identified by the given MongoFile
     * 
     * @param mongoFile
     * @throws IllegalArgumentException
     * @throws MongoException
     * @throws IOException
     */
    public void remove(final MongoFile mongoFile, final boolean async) throws IOException {

        if (mongoFile == null) {
            throw new IllegalArgumentException("mongoFile cannot be null");
        }
        remove(mongoFile.getURL(), async);
    }

    /**
     * Remove a file from the datastore identified by the given MongoFileUrl
     * 
     * NOTE: this is not asynchronous
     * 
     * @param url
     * @throws IllegalArgumentException
     * @throws MongoException
     */
    public void remove(final MongoFileUrl url) {

        remove(url, false);
    }

    /**
     * Remove a file from the datastore identified by the given MongoFileUrl
     * 
     * @param url
     *            - the MongoFileUrl
     * @param async
     *            - should the delete be asynchroized
     * 
     * @throws IOException
     *             if an error occurs during reading and/or writing
     * @throws IllegalArgumentException
     *             if required parameters are null
     */
    public void remove(final MongoFileUrl url, final boolean async) {

        if (url == null) {
            throw new IllegalArgumentException("mongoFileUrl cannot be null");
        }

        Document filesQuery = new Document().append("_id", url.getMongoFileId());

        Document chunksQuery = new Document("files_id", url.getMongoFileId());

        if (async) {
            setExpiresAt(filesQuery, chunksQuery, new Date(), false);
        } else {
            WriteResult writeResult = filesCollection.find(filesQuery).remove();
            if (writeResult.getCount() > 0) {
                chunksCollection.find(chunksQuery).remove();
            }
        }
    }

    /**
     * Delete all files that match the given criteria
     * 
     * NOTE: this is not asynchronous
     * 
     * @param query
     */
    public void remove(final Document query) {

        remove(query, false);
    }

    /**
     * Delete all files that match the given criteria
     * 
     * This code was taken from -- https://github.com/mongodb/mongo-java-driver/pull/171
     * 
     * @param query
     *            the selection criteria
     * @param async
     *            - can the file be deleted asynchronously
     * 
     * @throws IllegalArgumentException
     *             if required parameters are null
     */
    public void remove(final Document query, final boolean async) {

        if (query == null) {
            throw new IllegalArgumentException("query can not be null");
        }
        // can't remove chunks without files_id thus keep them
        List<ObjectId> filesIds = new ArrayList<ObjectId>();
        for (MongoFile f : find(query)) {
            filesIds.add(f.getId());
        }

        Document chunksQuery = new Document("files_id", new Document("$in", filesIds));

        if (async) {
            setExpiresAt(query, chunksQuery, new Date(), true);
        } else {
            // remove files from bucket
            WriteResult writeResult = getFilesCollection().find(query).remove();
            if (writeResult.getCount() > 0) {
                // then remove chunks, for those file objects
                getChunksCollection().find(chunksQuery).remove();
            }
        }
    }

    private void setExpiresAt(final Document filesQuery, final Document chunksQuery, final Date when, final boolean multi) {

        // files collection
        Document filesUpdate = new Document()//
                .append(MongoFileConstants.expireAt.toString(), when)//
                .append(MongoFileConstants.deleted.toString(), Boolean.TRUE);
        filesUpdate = new Document().append("$set", filesUpdate);
        getFilesCollection().find(filesQuery)//
                .withWriteConcern(WriteConcern.JOURNALED)//
                .update(filesUpdate);

        // chunks collection - wait until the file objects are removed
        Document chunksUpdate = new Document()//
                .append(MongoFileConstants.expireAt.toString(), when);
        chunksUpdate = new Document("$set", chunksUpdate);

        getFilesCollection().find(chunksQuery)//
                .withWriteConcern(WriteConcern.JOURNALED)//
                .update(chunksUpdate);
    }

    private Document deletedFileCheck(final Document file) {

        if (new MongoFile(this, file).isDeleted()) {
            return null;
        }
        return file;
    }

    //
    // collection getters
    // /////////////////////////

    /**
     * The underlying MongoDB collection object for files
     * 
     * @return the DBCollection object
     */
    public MongoCollection<Document> getFilesCollection() {

        return filesCollection;
    }

    /**
     * The underlying MongoDB collection object
     * 
     * @return the DBCollection object
     */
    public MongoCollection<Document> getChunksCollection() {

        return chunksCollection;
    }

    //
    // toString
    // //////////////////

    @Override
    public String toString() {

        return String.format("MongoFileStore [filesCollection=%s, chunksCollection=%s,%n  config=%s%n]", filesCollection,
                chunksCollection, config.toString());
    }

}
