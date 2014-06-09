package org.mongodb.file;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.mongodb.Document;
import org.mongodb.MongoCursor;
import org.mongodb.ServerCursor;
import org.mongodb.connection.ServerAddress;

/**
 * This class is a surrogate holder for the actual MongoCursor object
 * underneath. It store the
 * 
 * @author David Buschman
 * 
 */
public class MongoFileCursor implements MongoCursor<MongoFile>, Iterator<MongoFile>, Iterable<MongoFile> {

    private final MongoCursor<Document> cursor;
    private final MongoFileStore store;

    /* package */MongoFileCursor(final MongoFileStore store, final MongoCursor<Document> cursor) {

        this.store = store;
        this.cursor = cursor;

    }

    @Override
    public Iterator<MongoFile> iterator() {
        return this;
    }

    @Override
    public MongoFile next() {

        return new MongoFile(store, cursor.next());
    }

    @Override
    public boolean hasNext() {

        return cursor.hasNext();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public void close() {

        cursor.close();
    }

    @Override
    public ServerAddress getServerAddress() {

        return cursor.getServerAddress();
    }

    @Override
    public ServerCursor getServerCursor() {
        return cursor.getServerCursor();
    }

    /**
     * This method will iterate the cursor a return all of the object from the
     * cursor. Be sure you know the size of your result set.
     * 
     * @return a List<MongoFile> objects
     */
    public List<MongoFile> toList() {

        List<MongoFile> files = new ArrayList<MongoFile>();
        try {
            while (cursor.hasNext()) {
                files.add(new MongoFile(store, cursor.next()));
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return files;
    }

    //
    // toString
    // /////////////////////////
    @Override
    public String toString() {

        return String.format("MongoFileCursor [ store=%s, %n  cursor=%s%n]", store, cursor);
    }

}
