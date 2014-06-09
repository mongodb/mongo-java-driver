package org.mongodb.file.url;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import org.bson.types.ObjectId;
import org.mongodb.file.util.FileUtil;
import org.mongodb.file.writing.CompressionMediaTypes;

/**
 * 
 * mongoFile[:gz]:fileName.pdf?id#application/pdf
 * 
 * and is mapped to the following URL fields
 * 
 * protocol[:host]:path?query#ref
 * 
 * @author David Buschman
 * 
 */
public class MongoFileUrl {

    public static final String PROTOCOL = "mongofile";
    public static final String GZ = "gz";

    private URL url;

    // factories and helpers
    public static final MongoFileUrl construct(final ObjectId id, final String fileName, final String mediaType,
            final boolean compress) throws MalformedURLException {

        return construct(Parser.construct(id, fileName, mediaType, null, compress));
    }

    public static final MongoFileUrl construct(final ObjectId id, final String fileName, final String mediaType,
            final String compressionFormat) throws MalformedURLException {

        return construct(id, fileName, mediaType, compressionFormat, true);
    }

    public static final MongoFileUrl construct(final ObjectId id, final String fileName, final String mediaType,
            final String compressionFormat, final boolean compress) throws MalformedURLException {

        return construct(Parser.construct(id, fileName, mediaType, compressionFormat, compress));
    }

    public static final MongoFileUrl construct(final String spec) throws MalformedURLException {

        return construct(Parser.construct(spec));
    }

    /**
     * Construct a MogoFile object from the given URL, it will be tested from
     * validity
     * 
     * @param url
     * @return a MongoFile object for this URL
     */
    public static final MongoFileUrl construct(final URL url) {

        if (url == null) {
            throw new IllegalArgumentException("url cannot be null");
        }

        if (!url.getProtocol().equals(PROTOCOL)) {
            throw new IllegalStateException(String.format("Only %s protocal is valid to be wrapped", PROTOCOL));
        }
        return new MongoFileUrl(url);
    }

    /**
     * Is the given spec a valid MongoFile URL
     * 
     * @param spec
     * @return true if the spec is a valid URL
     */
    public static final boolean isValidUrl(final String spec) {

        try {
            return (null != construct(spec));
        } catch (Throwable t) {
            return false;
        }

    }

    // CTOR - not visible, use construct methods above
    /* package */MongoFileUrl(final String spec) throws MalformedURLException {

        this.url = new URL(null, spec, new Handler());
    }

    // CTOR- not visible, use construct methods above
    /* package */MongoFileUrl(final URL url) {

        this.url = url;
    }

    // toString
    @Override
    public String toString() {

        return this.url.toString();
    }

    // getters

    /**
     * Returns the 'attachment' protocol string
     * 
     * @return the protocol
     */
    public String getProtocol() {

        return url.getProtocol();
    }

    /**
     * Returns the full URL object
     * 
     * @return the URL object
     */
    public URL getUrl() {

        return this.url;
    }

    /**
     * Returns the lookup(Storage) Id from the URL
     * 
     * @return the primary key to the mongoFS system
     */
    public ObjectId getMongoFileId() {

        return new ObjectId(url.getQuery());
    }

    /**
     * Returns the full path specified in the URL
     * 
     * @return the full file path
     */
    public String getFilePath() {

        return url.getPath();
    }

    /**
     * Return just the last segment in the file path
     * 
     * @return just the filename
     */
    public String getFileName() {

        return new File(url.getPath()).getName();
    }

    /**
     * Returns the extension on the filename
     * 
     * @return the extension on the filename
     */
    public String getExtension() {

        // FindBugs, forced removal of null check
        return FileUtil.getExtension(new File(url.getPath())).toLowerCase();
    }

    /**
     * Returns the media type specified on the URL
     * 
     * @return the media type for the file
     */
    public String getMediaType() {

        return url.getRef();
    }

    /**
     * Returns the compression format to the stored data, null if not
     * compression
     * 
     * @return the compression format
     */
    public String getCompressionFormat() {

        return url.getHost();
    }

    //
    // boolean helpers
    //

    /**
     * Is the data stored in the file compressed in the datastore
     * 
     * @return true if compressed, false otherwise
     */
    public boolean isStoredCompressed() {

        if (url.getHost() != null && url.getHost().equals(GZ)) {
            return true;
        }

        return false;
    }

    /**
     * Is the data compressible based on the media type of the file. This may
     * differ from what is stored in the datasstore
     * 
     * @return true if the data is already compressed based on its media-type
     */
    public boolean isDataCompressed() {

        return !CompressionMediaTypes.isCompressable(getMediaType());
    }

    public boolean isSupportedProtocol(final String protocol) {

        if (url.getProtocol().equals(PROTOCOL)) {
            return true;
        }

        // unknown
        return false;
    }
}
