package org.mongodb.file;

import java.util.Set;
import java.util.TreeSet;

/**
 * 
 * @author David Buschman
 * 
 */
public enum MongoFileConstants {

    _id(true), //
    filename(true), //
    contentType(true), //
    chunkSize(true), //
    length(true), //
    uploadDate(true), //
    aliases(true), //
    md5(true), //
    metadata(true), //
    chunkCount(false), //
    compressedLength(false), //
    compressionRatio(false), //
    compressionFormat(false), //
    expireAt(false), //
    deleted(false);

    private boolean core = false;

    private MongoFileConstants(final boolean core) {

        this.core = core;
    }

    public static Set<String> getFields(final boolean mongoFS) {

        Set<String> set = new TreeSet<String>();

        for (MongoFileConstants current : MongoFileConstants.values()) {
            if (current.core || mongoFS) {
                set.add(current.name());
            }
        }

        return set;
    }

}
