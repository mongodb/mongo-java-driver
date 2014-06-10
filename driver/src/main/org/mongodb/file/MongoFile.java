package org.mongodb.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.bson.types.ObjectId;
import org.mongodb.Document;
import org.mongodb.MongoException;
import org.mongodb.file.reading.CountingInputStream;
import org.mongodb.file.reading.FileChunksInputStreamSource;
import org.mongodb.file.url.MongoFileUrl;
import org.mongodb.file.url.Parser;
import org.mongodb.file.util.BytesCopier;
import org.mongodb.file.writing.InputFile;

/**
 * Object to hold the state of the file's metatdata. To persist this outside of MongoFS, use the getURL() and persist that.
 * 
 * @author David Buschman
 * 
 */
public class MongoFile implements InputFile {

    private Document surrogate;

    private MongoFileStore store;

    private boolean compress = true;

    /**
     * Construct a MongoFile object for reading data
     * 
     * @param store
     * @param o
     */
    /* package */MongoFile(final MongoFileStore store, final Document surrogate) {

        this.store = store;
        this.surrogate = surrogate;
        this.compress = (surrogate == null) ? false : (surrogate.getString(MongoFileConstants.compressionFormat) != null);
    }

    /**
     * Construct a MongoFile object for writing data
     * 
     * @param collection
     * @param url
     */
    /* package */MongoFile(final MongoFileStore store, final MongoFileUrl url, final long chunkSize, final boolean compress) {

        this.store = store;
        this.compress = compress;

        surrogate = new Document();
        surrogate.put(MongoFileConstants._id.toString(), url.getMongoFileId());
        surrogate.put(MongoFileConstants.uploadDate.toString(), new Date());

        surrogate.put(MongoFileConstants.chunkSize.toString(), chunkSize);
        surrogate.put(MongoFileConstants.filename.toString(), url.getFilePath());
        surrogate.put(MongoFileConstants.contentType.toString(), url.getMediaType());
        if (url.getCompressionFormat() != null) {
            surrogate.put(MongoFileConstants.compressionFormat.toString(), url.getCompressionFormat());
        }
    }

    //
    // logic methods
    // //////////////////
    private String getBucketName() {

        return store.getFilesCollection().getName().split("\\.")[0];
    }

    /**
     * Saves the file entry to the files collection
     * 
     * @throws MongoException
     */
    public void save() {

        store.getFilesCollection().save(surrogate);
    }

    /**
     * Verifies that the MD5 matches between the database and the local file. This should be called after transferring a file.
     * 
     * @throws MongoException
     */
    public void validate() {

        MongoFileConstants md5key = MongoFileConstants.md5;
        String md5 = getString(md5key);
        if (md5 == null) {
            throw new MongoException("no md5 stored");
        }

        Document cmd = new Document("filemd5", get(MongoFileConstants._id));
        cmd.put("root", getBucketName());
        Document res = store.getFilesCollection().getDatabase().executeCommand(cmd).getResponse();
        if (res != null && res.containsKey(md5key.toString())) {
            String m = res.get(md5key.toString()).toString();
            if (m.equals(md5)) {
                return;
            }
            throw new MongoException("md5 differ.  mine [" + md5 + "] theirs [" + m + "]");
        }

        // no md5 from the server
        throw new MongoException("no md5 returned from server: " + res);

    }

    public MongoFileUrl getURL() throws MalformedURLException {

        if (surrogate != null) {
            URL url = Parser.construct(getId(), getFilename(), getContentType(),
                    (String) get(MongoFileConstants.compressionFormat.toString()), false);
            return MongoFileUrl.construct(url);
        }
        return MongoFileUrl.construct(getId(), getFilename(), getContentType(),
                (String) get(MongoFileConstants.compressionFormat.toString()), compress);
    }

    /**
     * Return an input stream to read the file content data from
     * 
     * @return an input stream to read from
     * 
     * @throws IOException
     */
    public final InputStream getInputStream() throws IOException {

        // returned <- counting <- chunks
        //
        // or
        //
        // returned <- gzip <- counting <- chunks
        InputStream returned = new FileChunksInputStreamSource(store, this);
        returned = new CountingInputStream(this, returned);

        if (getURL().isStoredCompressed()) {
            returned = new GZIPInputStream(returned);
        }

        return returned;
    }

    /**
     * Copy the content to the given output stream
     * 
     * @param out
     *            the output stream to write to
     * 
     * @param flush
     *            should the output stream be flush when all the data has been written.
     * 
     * @throws IOException
     */
    public OutputStream readInto(final OutputStream out, final boolean flush) throws IOException {

        new BytesCopier(getInputStream(), out).transfer(flush);

        return out;
    }

    //
    // read-only fields
    // ///////////////////////

    /**
     * Returns the number of chunks that store the file data.
     * 
     * @return number of chunks
     */
    public int getChunkCount() {

        if (surrogate == null) {
            throw new IllegalArgumentException("Cannot get chunk count before data is written");
        }

        // for compatibility with legacy GridFS implementations, if -1 comes
        // back, then legacy file
        int chunkCount = getInt(MongoFileConstants.chunkCount, -1);
        if (chunkCount == -1) {
            chunkCount = (int) Math.ceil((double) getLength() / getChunkSize());
        }
        return chunkCount;
    }

    /**
     * Gets the file's length.
     * 
     * @return the length of the file
     */
    public long getLength() {

        return getLong(MongoFileConstants.length);
    }

    /**
     * Gets the observed MD5 during transfer
     * 
     * @return md5
     */
    public String getMD5() {

        return getString(MongoFileConstants.md5);
    }

    /**
     * Gets the id.
     * 
     * @return the id of the file.
     */
    public ObjectId getId() {

        return (ObjectId) surrogate.get("_id");
    }

    /**
     * Gets the filename.
     * 
     * @return the name of the file
     */
    public String getFilename() {

        return getString(MongoFileConstants.filename);
    }

    /**
     * Gets the content type.
     * 
     * @return the content type
     */
    public String getContentType() {

        return getString(MongoFileConstants.contentType);
    }

    /**
     * Gets the size of a chunk.
     * 
     * @return the chunkSize
     */
    public int getChunkSize() {

        return getInt(MongoFileConstants.chunkSize);
    }

    /**
     * Gets the upload date.
     * 
     * @return the date
     */
    public Date getUploadDate() {

        return (Date) get(MongoFileConstants.uploadDate);
    }

    /**
     * Has the file been marked deleted but not yet removed from the store
     * 
     * @return true is the file is scheduled for deletion
     */
    public boolean isDeleted() {

        return getBoolean(MongoFileConstants.deleted, false);
    }

    //
    // getters and setters
    // /////////////////////////

    /**
     * Gets the aliases from the metadata. note: to set aliases, call put( "aliases" , List<String> )
     * 
     * @return list of aliases
     */
    @SuppressWarnings("unchecked")
    public List<String> getAliases() {

        return (List<String>) get(MongoFileConstants.aliases);
    }

    public void setAliases(final List<String> aliases) {

        put(MongoFileConstants.aliases, aliases);
    }

    /**
     * Gets the file metadata.
     * 
     * @return the metadata
     */
    public Document getMetaData() {

        return (Document) get(MongoFileConstants.metadata);
    }

    /**
     * Gets the file metadata.
     * 
     * @param metadata
     *            metadata to be set
     */
    public void setMetaData(final Document metadata) {

        put(MongoFileConstants.metadata, metadata);
    }

    /**
     * Add an object to the metadata subclass
     * 
     * @param key
     * @param value
     */
    public Object setInMetaData(final String key, final Object value) {

        Document object = (Document) get(MongoFileConstants.metadata);
        if (object == null) {
            object = new Document();
            setMetaData(object);
        }
        return object.put(key, value);
    }

    /**
     * Internal use only
     * 
     * @param when
     */
    /* package */void setExpiresAt(final Date when) {

        put(MongoFileConstants.expireAt, when);
    }

    /**
     * Return the expiration date for this file.
     * 
     * @return the expiration date for the file, null if is has none
     */
    public Date getExpiresAt() {

        return getDate(MongoFileConstants.expireAt, null);
    }

    //
    // helpers
    // /////////////////

    /**
     * Put a value into the object for a given key
     * 
     * @param key
     * @param value
     * @return the previous value
     */
    public Object put(final String key, final Object value) {

        if (key == null) {
            throw new RuntimeException("key should never be null");
        }

        return surrogate.put(key, value);
    }

    /**
     * Set a value based on the defined MongoFileConstants
     * 
     * @param key
     * @param value
     * @return the current value, or the default if its null
     */
    public Object put(final MongoFileConstants key, final Object value) {

        if (key == null) {
            throw new RuntimeException("key should never be null");
        }

        return surrogate.put(key.toString(), value);
    }

    /**
     * Get an the value on any attribute in the system
     * 
     * @param key
     * @return the current value
     */
    public Object get(final String key) {

        if (key == null) {
            throw new IllegalArgumentException("Key should never be null");
        }
        return surrogate.get(key);
    }

    /**
     * Get an the value on any attribute in the system
     * 
     * @param key
     * @return the current value
     */
    public Object get(final MongoFileConstants key) {

        if (key == null) {
            throw new IllegalArgumentException("Key should never be null");
        }
        return surrogate.get(key.toString());
    }

    /**
     * Return the value for the given key as a string
     * 
     * @param key
     * 
     * @return the string value
     */
    public String getString(final MongoFileConstants key) {

        return (String) surrogate.get(key.toString());
    }

    /**
     * Return the value for the given key as a integer
     * 
     * @param key
     * 
     * @return the key's value as an integer
     */
    public int getInt(final MongoFileConstants key) {

        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        Object value = surrogate.get(key.toString());
        if (value == null) {
            return -1;
        }

        return Integer.parseInt(value.toString());
    }

    /**
     * Return the value for the given key as a long
     * 
     * @param key
     * 
     * @return the value as a long
     */
    public long getLong(final MongoFileConstants key) {

        if (key == null) {
            throw new IllegalArgumentException("key cannot be null");
        }

        Object value = surrogate.get(key.toString());
        if (value == null) {
            return -1;
        }

        return Long.parseLong(value.toString());
    }

    /**
     * Returns the value of a field as an <code>int</code>.
     * 
     * @param key
     *            the field to look for
     * @param def
     *            the default to return
     * @return the field value (or default)
     */
    public int getInt(final MongoFileConstants key, final int def) {

        if (surrogate == null) {
            return def;
        }

        Integer value = surrogate.getInteger(key.toString());
        return value != null ? value : def;
    }

    /**
     * Returns the value of a field as an <code>long</code>.
     * 
     * @param key
     *            the field to look for
     * @param def
     *            the default to return
     * @return the field value (or default)
     */
    public long getLong(final MongoFileConstants key, final long def) {

        if (surrogate == null) {
            return def;
        }

        Long value = surrogate.getLong(key.toString());
        return value != null ? value : def;
    }

    /**
     * Returns the value of a field as an <code>double</code>.
     * 
     * @param key
     *            the field to look for
     * @param def
     *            the default to return
     * @return the field value (or default)
     */
    public double getDouble(final MongoFileConstants key, final double def) {

        if (surrogate == null) {
            return def;
        }

        Double value = surrogate.getDouble(key.toString());
        return value != null ? value : def;
    }

    /**
     * Returns the value of a field as a string
     * 
     * @param key
     *            the field to look up
     * @param def
     *            the default to return
     * @return the value of the field, converted to a string
     */
    public String getString(final MongoFileConstants key, final String def) {

        if (surrogate == null) {
            return def;
        }

        String value = surrogate.getString(key.toString());
        return value != null ? value : def;
    }

    /**
     * Returns the value of a field as a boolean
     * 
     * @param key
     *            the field to look up
     * @param def
     *            the default value in case the field is not found
     * @return the value of the field, converted to a string
     */
    public boolean getBoolean(final MongoFileConstants key, final boolean def) {

        if (surrogate == null) {
            return def;
        }

        Object foo = surrogate.get(key.toString());
        if (foo == null) {
            return def;
        }
        if (foo instanceof Number) {
            return ((Number) foo).intValue() > 0;
        }
        if (foo instanceof Boolean) {
            return ((Boolean) foo).booleanValue();
        }
        throw new IllegalArgumentException("can't coerce to bool:" + foo.getClass());
    }

    /**
     * Returns the object id or def if not set.
     * 
     * @param field
     *            The field to return
     * @param def
     *            the default value in case the field is not found
     * @return The field object value or def if not set.
     */
    public ObjectId getObjectId(final MongoFileConstants field, final ObjectId def) {

        if (surrogate == null) {
            return def;
        }

        final Object foo = surrogate.get(field.toString());
        return (foo != null) ? (ObjectId) foo : def;
    }

    /**
     * Returns the date or def if not set.
     * 
     * @param field
     *            The field to return
     * @param def
     *            the default value in case the field is not found
     * @return The field object value or def if not set.
     */
    public Date getDate(final MongoFileConstants field, final Date def) {

        if (surrogate == null) {
            return def;
        }

        final Object foo = surrogate.get(field.toString());
        return (foo != null) ? (Date) foo : def;
    }

    /**
     * Does a key exist in the object
     * 
     * @param key
     * 
     * @return true if it exists
     */
    public boolean containsKey(final String key) {

        return keySet().contains(key);
    }

    private Set<String> keySet() {

        Set<String> keys = new HashSet<String>();
        keys.addAll(MongoFileConstants.getFields(true));
        keys.addAll(surrogate.keySet());
        return keys;
    }

    @Override
    public String toString() {

        return surrogate.toString();
    }

    public boolean isCompressed() {

        return compress;
    }

}
